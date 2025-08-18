# etl/full_seed.py
from __future__ import annotations
import os, asyncio, time, random, sys, json
from typing import Optional, Iterable

import psycopg
import httpx
from lxml import etree
from urllib.parse import quote

# ================== CONFIG ==================
DATERIUM_USER_ID = os.getenv("DATERIUM_USER_ID", "").strip()
# Usamos DATABASE_URL y, si es interno (.internal) y estás fuera de Railway, permite usar DATABASE_PUBLIC_URL.
def _effective_dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("PGDATABASE_URL") or ""
    if (".internal" in dsn or "railway.internal" in dsn) and os.getenv("DATABASE_PUBLIC_URL"):
        return os.getenv("DATABASE_PUBLIC_URL")
    return dsn

MAX_CONCURRENCY = int(os.getenv("SEED_CONCURRENCY", "5"))
RATE_DELAY      = float(os.getenv("SEED_RATE_DELAY", "0.4"))
BATCH_COMMIT    = int(os.getenv("SEED_BATCH_COMMIT", "50"))

# ================== LOG ==================
def log(msg: str):
    print(msg, flush=True, file=sys.stdout)

def log_json(**kv):
    log(json.dumps(kv, ensure_ascii=False))

# ================== DB HELPERS ==================
def db_conn():
    dsn = _effective_dsn()
    if not dsn:
        raise SystemExit("DATABASE_URL / DATABASE_PUBLIC_URL no está definido")
    return psycopg.connect(dsn, autocommit=False)

def upsert_brand(cur, name: Optional[str], logo_url: Optional[str]) -> Optional[int]:
    if not name: return None
    cur.execute("""
        INSERT INTO brands(name, logo_url)
        VALUES (%s, %s)
        ON CONFLICT (name)
        DO UPDATE SET logo_url = COALESCE(EXCLUDED.logo_url, brands.logo_url)
        RETURNING id
    """, (name, logo_url))
    return cur.fetchone()[0]

def upsert_family(cur, name: Optional[str], parent_id: Optional[int] = None) -> Optional[int]:
    if not name: return None
    cur.execute("""
        INSERT INTO families(name, parent_id)
        VALUES (%s, %s)
        ON CONFLICT (name)
        DO UPDATE SET parent_id = COALESCE(EXCLUDED.parent_id, families.parent_id)
        RETURNING id
    """, (name, parent_id))
    return cur.fetchone()[0]

def upsert_product(cur,
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

def upsert_image(cur, product_id: int, url: str, is_primary: bool):
    cur.execute("""
        INSERT INTO product_images(product_id, url, is_primary)
        VALUES (%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (product_id, url, is_primary))

def ensure_cursor_table(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ingest_cursor (
          id          BIGSERIAL PRIMARY KEY,
          strategy    TEXT NOT NULL,
          cursor_key  TEXT NOT NULL,
          updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_cursor_strategy
          ON ingest_cursor(strategy);
        """)
    conn.commit()

def get_cursor(conn: psycopg.Connection, strategy: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT cursor_key FROM ingest_cursor WHERE strategy = %s", (strategy,))
        row = cur.fetchone()
        return row[0] if row else None

def set_cursor(conn: psycopg.Connection, strategy: str, key: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingest_cursor(strategy, cursor_key, updated_at)
            VALUES (%s,%s,NOW())
            ON CONFLICT (strategy) DO UPDATE SET cursor_key = EXCLUDED.cursor_key, updated_at = NOW()
        """, (strategy, key))
    conn.commit()

# ================== HTTP/Parse ==================
def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(connect=8.0, read=50.0, write=10.0, pool=100.0),
        headers={"User-Agent": "CompraInteligente/1.0", "Accept": "application/xml"},
    )

async def fetch_query(client: httpx.AsyncClient, query: str) -> Optional[bytes]:
    if not DATERIUM_USER_ID:
        raise SystemExit("Falta DATERIUM_USER_ID")
    url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(DATERIUM_USER_ID)}&searchbox={quote(query)}"
    try:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        return r.content
    except Exception:
        return None

def parse_float(txt: Optional[str]) -> Optional[float]:
    if not txt: return None
    try:
        return float(str(txt).replace(",", "."))
    except Exception:
        return None

def parse_and_upsert(conn: psycopg.Connection, xml_bytes: bytes) -> int:
    root = etree.fromstring(xml_bytes)
    inserted = 0
    with conn.cursor() as cur:
        for ficha in root.xpath(".//ficha"):
            id_txt = ficha.findtext("id")
            idcat  = ficha.get("idcatalogo")
            daterium_id = None
            for candidate in (id_txt, idcat):
                if candidate and str(candidate).strip().isdigit():
                    daterium_id = int(str(candidate).strip()); break

            nombre = (ficha.findtext("nombre") or "").strip()
            if not nombre:
                continue
            descripcion = (ficha.findtext("descripcion") or "") or (ficha.findtext("descripcioncorta") or "")
            descripcion = (descripcion or "").strip()

            marca_name = (ficha.findtext("marca") or "").strip() or None
            logo_marca = (ficha.findtext("logo_marca") or "").strip() or None

            familia_name    = (ficha.findtext("familia") or "").strip() or None
            subfamilia_name = (ficha.findtext("subfamilia") or "").strip() or None

            thumb    = (ficha.findtext("thumb") or "").strip() or None
            img280   = (ficha.findtext("img280x240") or "").strip() or None
            img500   = (ficha.findtext("img500x500") or "").strip() or None
            image_url = img500 or img280 or thumb

            ean = None; pvp = None
            ref = ficha.find(".//referencias/referencia")
            if ref is not None:
                ean = (ref.findtext("ean") or "").strip() or None
                pvp = parse_float(ref.findtext("pvp"))

            brand_id  = upsert_brand(cur, marca_name, logo_marca)
            parent_id = upsert_family(cur, familia_name, None) if familia_name else None
            family_id = upsert_family(cur, subfamilia_name, parent_id) if subfamilia_name else parent_id

            pid = upsert_product(cur, daterium_id, nombre, descripcion, brand_id, family_id, ean, None, pvp, thumb, image_url)
            if thumb:  upsert_image(cur, pid, thumb,  image_url == thumb)
            if img280: upsert_image(cur, pid, img280, image_url == img280)
            if img500: upsert_image(cur, pid, img500, image_url == img500)
            inserted += 1
    return inserted

# ================== Generadores de claves ==================
def gen_ngrams() -> Iterable[str]:
    alpha = "abcdefghijklmnopqrstuvwxyz"
    for c in alpha:
        yield c
    for a in alpha:
        for b in alpha:
            yield a + b
    common = ["bro","pun","ato","tor","per","dis","sie","lla","adh","tiv","cer","met"]
    for c in common:
        yield c

def gen_trigrams() -> Iterable[str]:
    alpha = "abcdefghijklmnopqrstuvwxyz"
    for a in alpha:
        for b in alpha:
            for c in alpha:
                yield a + b + c

def gen_digits() -> Iterable[str]:
    for d in range(0, 100):
        yield str(d)
    for p in ["84","80","50","40"]:
        yield p

def gen_from_db(conn: psycopg.Connection, table: str) -> Iterable[str]:
    q = "SELECT name FROM " + ("brands" if table == "brands" else "families")
    with conn.cursor() as cur:
        cur.execute(q)
        for (name,) in cur.fetchall():
            if name: yield name

# ================== Runner por estrategia ==================
async def run_strategy(strategy: str):
    if not DATERIUM_USER_ID:
        raise SystemExit("Falta DATERIUM_USER_ID")

    conn = db_conn()
    ensure_cursor_table(conn)

    # Generar claves
    if strategy == "ngrams":
        keys = list(gen_ngrams())
    elif strategy == "digits":
        keys = list(gen_digits())
    elif strategy == "brands":
        keys = list(gen_from_db(conn, "brands"))
    elif strategy == "families":
        keys = list(gen_from_db(conn, "families"))
    elif strategy == "trigrams":
        keys = list(gen_trigrams())
    else:
        conn.close()
        raise SystemExit(f"Estrategia no soportada: {strategy}")

    # Cursor resumible
    cursor_key = get_cursor(conn, strategy)
    if cursor_key and cursor_key in keys:
        start = keys.index(cursor_key)
        keys = keys[start:]
    random.shuffle(keys)

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    client = make_client()
    processed = 0

    async def worker(term: str):
        nonlocal processed
        async with sem:
            await asyncio.sleep(RATE_DELAY)
            xml = await fetch_query(client, term)
            if not xml:
                processed += 1
                return
            with db_conn() as c2:
                try:
                    n = parse_and_upsert(c2, xml)
                    c2.commit()
                    log_json(evt="batch_done", strategy=strategy, term=term, inserted_or_updated=n)
                except Exception as e:
                    c2.rollback()
                    log_json(evt="batch_error", strategy=strategy, term=term, error=str(e))
            processed += 1
            set_cursor(conn, strategy, term)

    try:
        for i in range(0, len(keys), 50):
            batch = keys[i:i+50]
            await asyncio.gather(*[worker(k) for k in batch])
            log_json(evt="progress", strategy=strategy, processed=processed, of=len(keys))
    finally:
        await client.aclose()
        conn.close()

# ================== Loop infinito ==================
def parse_modes(csv: str) -> list[str]:
    allowed = {"brands","families","ngrams","digits","trigrams"}
    modes = [m.strip() for m in csv.split(",") if m.strip()]
    return [m for m in modes if m in allowed]

def run_cycle(modes: list[str], idle_sleep: int = 600, max_errors: int = 10):
    errors = 0
    while True:
        try:
            with db_conn() as conn:
                ensure_cursor_table(conn)
        except Exception as e:
            log(f"[loop] ensure_cursor_table error: {e}")
            errors += 1
            if errors >= max_errors:
                log("[loop] demasiados errores. Saliendo.")
                return
            time.sleep(10)
            continue

        for m in modes:
            try:
                log(f"[loop] running strategy={m}")
                asyncio.run(run_strategy(m))
            except Exception as exc:
                log(f"[loop] error en {m}: {exc}")
                errors += 1
                if errors >= max_errors:
                    log("[loop] demasiados errores seguidos. Saliendo.")
                    return

        log(f"[loop] ciclo completo. Sleep {idle_sleep}s…")
        time.sleep(idle_sleep)

# ================== main ==================
def main():
    import argparse
    p = argparse.ArgumentParser(description="Ingestor Daterium → Postgres")
    p.add_argument("--mode", choices=["ngrams","digits","brands","families","trigrams"], help="Estrategia puntual")
    p.add_argument("--modes", type=str, help="CSV para --loop (p.ej. brands,families,ngrams,digits)")
    p.add_argument("--loop", action="store_true", help="Bucle infinito rotando estrategias")
    p.add_argument("--idle-sleep", type=int, default=600, help="Espera entre ciclos en --loop (s)")
    args = p.parse_args()

    if args.loop:
        modes = parse_modes(args.modes) if args.modes else ["brands","families","ngrams","digits"]
        log_json(evt="loop_start", modes=modes, idle_sleep=args.idle_sleep)
        run_cycle(modes, idle_sleep=args.idle_sleep)
    else:
        if not args.mode:
            raise SystemExit("Indica --mode o usa --loop")
        with db_conn() as conn:
            ensure_cursor_table(conn)
        log_json(evt="single_mode_start", mode=args.mode)
        asyncio.run(run_strategy(args.mode))

if __name__ == "__main__":
    main()