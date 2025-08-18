# etl/load_catalog.py
from __future__ import annotations
import os
from typing import Optional, Tuple
import httpx
from lxml import etree
import psycopg
from urllib.parse import quote

DATERIUM_USER_ID = os.getenv("DATERIUM_USER_ID", "").strip()

# Endpoint de búsqueda (usaremos varias queries para una carga inicial modesta)
DATERIUM_URL = "https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={uid}&searchbox={q}"

# Lista corta de semillas para prueba (luego ampliaremos)
SEED_QUERIES = [
    "tivoly", "broca", "punta", "atornillado", "anclaje", "disco", "llave",
    "sierra", "adhesivo", "tornillo", "tuerca", "arandela"
]

TIMEOUT = httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=5.0)

def connect_db() -> psycopg.Connection:
    dsn = os.getenv("DATABASE_URL") or os.getenv("PGDATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL no está definido")
    return psycopg.connect(dsn, autocommit=False)

def upsert_brand(cur: psycopg.Cursor, name: Optional[str], logo_url: Optional[str]) -> Optional[int]:
    if not name:
        return None
    cur.execute(
        """
        INSERT INTO brands(name, logo_url)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET logo_url = COALESCE(EXCLUDED.logo_url, brands.logo_url)
        RETURNING id
        """,
        (name, logo_url)
    )
    return cur.fetchone()[0]

def upsert_family(cur: psycopg.Cursor, name: Optional[str], parent_id: Optional[int] = None) -> Optional[int]:
    if not name:
        return None
    # upsert por nombre (único)
    cur.execute(
        """
        INSERT INTO families(name, parent_id)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET parent_id = COALESCE(EXCLUDED.parent_id, families.parent_id)
        RETURNING id
        """,
        (name, parent_id)
    )
    return cur.fetchone()[0]

def upsert_product(cur: psycopg.Cursor,
                   daterium_id: Optional[int],
                   name: str,
                   description: Optional[str],
                   brand_id: Optional[int],
                   family_id: Optional[int],
                   ean: Optional[str],
                   sku: Optional[str],
                   pvp: Optional[float],
                   thumb_url: Optional[str],
                   image_url: Optional[str]) -> int:
    cur.execute(
        """
        INSERT INTO products(daterium_id, name, description, brand_id, family_id, ean, sku, pvp, thumb_url, image_url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (daterium_id) DO UPDATE
          SET name = EXCLUDED.name,
              description = COALESCE(EXCLUDED.description, products.description),
              brand_id = COALESCE(EXCLUDED.brand_id, products.brand_id),
              family_id = COALESCE(EXCLUDED.family_id, products.family_id),
              ean = COALESCE(EXCLUDED.ean, products.ean),
              sku = COALESCE(EXCLUDED.sku, products.sku),
              pvp = COALESCE(EXCLUDED.pvp, products.pvp),
              thumb_url = COALESCE(EXCLUDED.thumb_url, products.thumb_url),
              image_url = COALESCE(EXCLUDED.image_url, products.image_url)
        RETURNING id
        """,
        (daterium_id, name, description, brand_id, family_id, ean, sku, pvp, thumb_url, image_url)
    )
    return cur.fetchone()[0]

def upsert_image(cur: psycopg.Cursor, product_id: int, url: str, is_primary: bool):
    cur.execute(
        """
        INSERT INTO product_images(product_id, url, is_primary)
        VALUES (%s,%s,%s)
        ON CONFLICT DO NOTHING
        """,
        (product_id, url, is_primary)
    )

def parse_float(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    try:
        return float(str(txt).replace(",", "."))
    except Exception:
        return None

def fetch_xml(query: str) -> Optional[bytes]:
    url = DATERIUM_URL.format(uid=quote(DATERIUM_USER_ID), q=quote(query))
    headers = {"User-Agent": "CompraInteligente/1.0", "Accept": "application/xml"}
    with httpx.Client(timeout=TIMEOUT, headers=headers) as c:
        r = c.get(url)
        if r.status_code != 200:
            return None
        return r.content

def load_query(conn: psycopg.Connection, query: str) -> int:
    xml_bytes = fetch_xml(query)
    if not xml_bytes:
        return 0
    root = etree.fromstring(xml_bytes)  # documento relativamente pequeño por query
    count = 0

    for ficha in root.xpath(".//ficha"):
        # Campos básicos
        try:
            idcatalogo_txt = ficha.get("idcatalogo")  # a veces id nº interno
        except Exception:
            idcatalogo_txt = None
        # El <id> suele contener el ident interno del producto
        id_txt = ficha.findtext("id")
        daterium_id = None
        for candidate in [id_txt, idcatalogo_txt]:
            if candidate and str(candidate).strip().isdigit():
                daterium_id = int(str(candidate).strip())
                break

        nombre = (ficha.findtext("nombre") or "").strip()
        if not nombre:
            continue
        descripcion = (ficha.findtext("descripcion") or "") or (ficha.findtext("descripcioncorta") or "")
        descripcion = (descripcion or "").strip()

        marca_name = (ficha.findtext("marca") or "").strip()
        logo_marca = (ficha.findtext("logo_marca") or "").strip() or None

        familia_name = (ficha.findtext("familia") or "").strip() or None
        subfamilia_name = (ficha.findtext("subfamilia") or "").strip() or None

        thumb = (ficha.findtext("thumb") or "").strip() or None
        img280 = (ficha.findtext("img280x240") or "").strip() or None
        img500 = (ficha.findtext("img500x500") or "").strip() or None
        image_url = img500 or img280 or thumb

        # Referencia principal (puede no venir)
        ean = None
        pvp = None
        ref = ficha.find(".//referencias/referencia")
        if ref is not None:
            ean = (ref.findtext("ean") or "").strip() or None
            pvp = parse_float(ref.findtext("pvp"))

        with conn.cursor() as cur:
            brand_id = upsert_brand(cur, marca_name or None, logo_marca)
            parent_id = None
            if familia_name:
                parent_id = upsert_family(cur, familia_name, None)
            family_id = None
            if subfamilia_name:
                family_id = upsert_family(cur, subfamilia_name, parent_id)
            else:
                family_id = parent_id

            pid = upsert_product(
                cur=cur,
                daterium_id=daterium_id,
                name=nombre,
                description=descripcion,
                brand_id=brand_id,
                family_id=family_id,
                ean=ean,
                sku=None,
                pvp=pvp,
                thumb_url=thumb,
                image_url=image_url
            )

            # Imágenes
            if thumb:
                upsert_image(cur, pid, thumb, is_primary=(image_url == thumb))
            if img280:
                upsert_image(cur, pid, img280, is_primary=(image_url == img280))
            if img500:
                upsert_image(cur, pid, img500, is_primary=(image_url == img500))

        conn.commit()
        count += 1

    return count

def main():
    if not DATERIUM_USER_ID:
        raise SystemExit("Falta DATERIUM_USER_ID en variables de entorno")

    inserted_total = 0
    with connect_db() as conn:
        # Log inicio
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ingest_log(source, items, started_at) VALUES (%s, %s, NOW()) RETURNING id",
                ("daterium", 0)
            )
            log_id = cur.fetchone()[0]
            conn.commit()

        for q in SEED_QUERIES:
            try:
                n = load_query(conn, q)
                inserted_total += n
                print(f"[OK] {q}: {n}")
            except Exception as ex:
                print(f"[WARN] {q}: {ex}")

        # Log fin
        with connect_db() as conn2:
            with conn2.cursor() as cur2:
                cur2.execute(
                    "UPDATE ingest_log SET items = %s, finished_at = NOW() WHERE id = %s",
                    (inserted_total, log_id)
                )
                conn2.commit()

    print(f"[DONE] Total insertados/actualizados: {inserted_total}")

if __name__ == "__main__":
    main()