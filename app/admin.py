# app/admin.py
from __future__ import annotations
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
import psycopg

router = APIRouter(tags=["admin"])

# ------- helpers -------
def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("PGDATABASE_URL")
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL missing")
    return dsn

def _check_token(token: str):
    expected = os.getenv("MIGRATION_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _load_sql_file(rel_path: str) -> str:
    """
    Carga el SQL desde `rel_path` respecto a la raíz del repo.
    Filtra líneas que empiecen por '#' (comentarios no válidos en Postgres).
    """
    sql_path = Path(__file__).resolve().parent.parent / rel_path
    if not sql_path.exists():
        raise HTTPException(status_code=500, detail=f"migration file not found: {rel_path}")
    lines = sql_path.read_text(encoding="utf-8").splitlines()
    cleaned = "\n".join(l for l in lines if not l.strip().startswith("#"))
    return cleaned

# ------- endpoints -------
@router.get("/admin/debug_token_status")
def debug_token_status():
    """No expone el token. Solo indica si está presente y su longitud."""
    val = os.getenv("MIGRATION_TOKEN", "")
    return {"present": bool(val), "length": len(val)}

@router.post("/admin/migrate")
def run_migration(token: str = Query(..., description="Security token")):
    """
    Ejecuta migrations/0002_catalog.sql en una transacción.
    Requiere ?token=...
    """
    _check_token(token)
    sql_text = _load_sql_file("migrations/0002_catalog.sql")

    try:
        with psycopg.connect(_dsn(), autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)
            conn.commit()
        return {"ok": True, "migrated": "0002_catalog.sql"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"migration failed: {e}")

@router.get("/admin/tables")
def list_tables(token: str = Query(..., description="Security token")):
    _check_token(token)
    try:
        with psycopg.connect(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tablename FROM pg_tables "
                    "WHERE schemaname='public' ORDER BY tablename"
                )
                rows = [r[0] for r in cur.fetchall()]
        return {"ok": True, "tables": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"list failed: {e}")

@router.get("/admin/count")
def count_tables(token: str = Query(..., description="Security token")):
    _check_token(token)
    try:
        with psycopg.connect(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM brands")
                brands = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM families")
                families = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM products")
                products = cur.fetchone()[0]

        return {"ok": True, "counts": {"brands": brands, "families": families, "products": products}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"count failed: {e}")
    # ====== CARGA BÁSICA (semilla) DESDE DATERIUM ======
import httpx
from lxml import etree
from urllib.parse import quote

SEED_QUERIES = [
    "tivoly", "broca", "punta", "atornillado", "anclaje",
    "disco", "llave", "sierra", "adhesivo", "tornillo", "tuerca", "arandela"
]

def _http():
    return httpx.Client(
        timeout=httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=5.0),
        headers={"User-Agent": "CompraInteligente/1.0", "Accept": "application/xml"},
    )

def _parse_float(txt: str | None) -> float | None:
    if not txt: return None
    try: return float(str(txt).replace(",", "."))
    except Exception: return None

def _upsert_brand(cur, name: str | None, logo_url: str | None) -> int | None:
    if not name: return None
    cur.execute("""
        INSERT INTO brands(name, logo_url)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET logo_url = COALESCE(EXCLUDED.logo_url, brands.logo_url)
        RETURNING id
    """, (name, logo_url))
    return cur.fetchone()[0]

def _upsert_family(cur, name: str | None, parent_id: int | None = None) -> int | None:
    if not name: return None
    cur.execute("""
        INSERT INTO families(name, parent_id)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET parent_id = COALESCE(EXCLUDED.parent_id, families.parent_id)
        RETURNING id
    """, (name, parent_id))
    return cur.fetchone()[0]

def _upsert_product(cur,
                    daterium_id: int | None,
                    name: str,
                    description: str | None,
                    brand_id: int | None,
                    family_id: int | None,
                    ean: str | None,
                    sku: str | None,
                    pvp: float | None,
                    thumb_url: str | None,
                    image_url: str | None) -> int:
    cur.execute("""
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
    """, (daterium_id, name, description, brand_id, family_id, ean, sku, pvp, thumb_url, image_url))
    return cur.fetchone()[0]

def _upsert_image(cur, product_id: int, url: str, is_primary: bool):
    cur.execute("""
        INSERT INTO product_images(product_id, url, is_primary)
        VALUES (%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (product_id, url, is_primary))

@router.post("/admin/seed_basic")
def seed_basic(token: str = Query(..., description="Security token"),
               queries: str = Query(None, description="CSV de términos. Si no, usa los de SEED_QUERIES")):
    """
    Carga básica: ejecuta varias búsquedas en Daterium y persiste marcas/familias/productos.
    Param:
      - token: MIGRATION_TOKEN
      - queries (opcional): 'tivoly,broca,disco'
    """
    _check_token(token)
    user_id = os.getenv("DATERIUM_USER_ID", "").strip()
    if not user_id:
        raise HTTPException(status_code=500, detail="Falta DATERIUM_USER_ID")

    q_list = [q.strip() for q in (queries.split(",") if queries else SEED_QUERIES) if q.strip()]
    total = 0

    with psycopg.connect(_dsn(), autocommit=False) as conn:
        with conn.cursor() as cur:
            for q in q_list:
                url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(user_id)}&searchbox={quote(q)}"
                with _http() as c:
                    r = c.get(url)
                    if r.status_code != 200:
                        continue
                    root = etree.fromstring(r.content)

                for ficha in root.xpath(".//ficha"):
                    id_txt = ficha.findtext("id")
                    idcat = ficha.get("idcatalogo")
                    daterium_id = None
                    for candidate in (id_txt, idcat):
                        if candidate and str(candidate).strip().isdigit():
                            daterium_id = int(str(candidate).strip())
                            break

                    nombre = (ficha.findtext("nombre") or "").strip()
                    if not nombre:
                        continue
                    descripcion = (ficha.findtext("descripcion") or "") or (ficha.findtext("descripcioncorta") or "")
                    descripcion = (descripcion or "").strip()

                    marca_name = (ficha.findtext("marca") or "").strip() or None
                    logo_marca = (ficha.findtext("logo_marca") or "").strip() or None

                    familia_name = (ficha.findtext("familia") or "").strip() or None
                    subfamilia_name = (ficha.findtext("subfamilia") or "").strip() or None

                    thumb = (ficha.findtext("thumb") or "").strip() or None
                    img280 = (ficha.findtext("img280x240") or "").strip() or None
                    img500 = (ficha.findtext("img500x500") or "").strip() or None
                    image_url = img500 or img280 or thumb

                    ean = None; pvp = None
                    ref = ficha.find(".//referencias/referencia")
                    if ref is not None:
                        ean = (ref.findtext("ean") or "").strip() or None
                        pvp = _parse_float(ref.findtext("pvp"))

                    brand_id = _upsert_brand(cur, marca_name, logo_marca)

                    parent_id = _upsert_family(cur, familia_name, None) if familia_name else None
                    family_id = _upsert_family(cur, subfamilia_name, parent_id) if subfamilia_name else parent_id

                    pid = _upsert_product(cur, daterium_id, nombre, descripcion, brand_id, family_id, ean, None, pvp, thumb, image_url)

                    if thumb:  _upsert_image(cur, pid, thumb,  image_url == thumb)
                    if img280: _upsert_image(cur, pid, img280, image_url == img280)
                    if img500: _upsert_image(cur, pid, img500, image_url == img500)

                    total += 1

            conn.commit()

    return {"ok": True, "inserted_or_updated": total, "queries": q_list}