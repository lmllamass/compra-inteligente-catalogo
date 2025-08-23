# etl/daterium_real.py
from __future__ import annotations
import os, asyncio, time, random, sys, json
from typing import Optional, Iterable, List, Dict, Any

import psycopg
import httpx
from lxml import etree
from urllib.parse import quote

# ================== CONFIG ==================
DATERIUM_USER_ID = os.getenv("DATERIUM_USER_ID", "").strip()

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

def ensure_tables(conn: psycopg.Connection):
    """Asegurar que todas las tablas necesarias existen"""
    with conn.cursor() as cur:
        # Tabla para referencias/SKUs múltiples
        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_references (
          id             BIGSERIAL PRIMARY KEY,
          product_id     BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
          reference_code TEXT NOT NULL,
          reference_type TEXT,                    -- 'sku', 'model', 'internal', etc.
          supplier_name  TEXT,                    -- proveedor específico
          is_primary     BOOLEAN DEFAULT FALSE,  -- referencia principal
          created_at     TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_product_refs_product ON product_references(product_id);
        CREATE INDEX IF NOT EXISTS idx_product_refs_code ON product_references(reference_code);
        CREATE UNIQUE INDEX IF NOT EXISTS ux_product_refs_product_code 
          ON product_references(product_id, reference_code);
        """)
        
        # Tabla para categorías AECOC
        cur.execute("""
        CREATE TABLE IF NOT EXISTS aecoc_categories (
          id           BIGSERIAL PRIMARY KEY,
          aecoc_id     TEXT NOT NULL UNIQUE,
          name         TEXT NOT NULL,
          parent_id    BIGINT REFERENCES aecoc_categories(id),
          level        INTEGER DEFAULT 0,
          created_at   TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_aecoc_parent ON aecoc_categories(parent_id);
        CREATE INDEX IF NOT EXISTS idx_aecoc_level ON aecoc_categories(level);
        """)
        
        # Tabla de relación producto-AECOC
        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_aecoc (
          id             BIGSERIAL PRIMARY KEY,
          product_id     BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
          aecoc_id       BIGINT NOT NULL REFERENCES aecoc_categories(id),
          created_at     TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE UNIQUE INDEX IF NOT EXISTS ux_product_aecoc 
          ON product_aecoc(product_id, aecoc_id);
        """)
        
        # Agregar campos a products si no existen
        cur.execute("""
        ALTER TABLE products 
        ADD COLUMN IF NOT EXISTS supplier_name TEXT,
        ADD COLUMN IF NOT EXISTS supplier_cif TEXT,
        ADD COLUMN IF NOT EXISTS relevancia NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS idcatalogo TEXT;
        """)
    
    conn.commit()

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
    # Normalizar nombre de familia
    name = name.strip()
    if name.lower() == "otros":
        name = "Otros"
    
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
                   thumb_url: Optional[str],
                   image_url: Optional[str],
                   supplier_name: Optional[str] = None,
                   supplier_cif: Optional[str] = None,
                   relevancia: Optional[float] = None,
                   idcatalogo: Optional[str] = None) -> int:
    """Producto con campos específicos de Daterium"""
    cur.execute("""
        INSERT INTO products(
            daterium_id, name, description, brand_id, family_id, 
            thumb_url, image_url, supplier_name, supplier_cif, 
            relevancia, idcatalogo
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (daterium_id) DO UPDATE
          SET name = EXCLUDED.name,
              description = COALESCE(EXCLUDED.description, products.description),
              brand_id = COALESCE(EXCLUDED.brand_id, products.brand_id),
              family_id = COALESCE(EXCLUDED.family_id, products.family_id),
              thumb_url = COALESCE(EXCLUDED.thumb_url, products.thumb_url),
              image_url = COALESCE(EXCLUDED.image_url, products.image_url),
              supplier_name = COALESCE(EXCLUDED.supplier_name, products.supplier_name),
              supplier_cif = COALESCE(EXCLUDED.supplier_cif, products.supplier_cif),
              relevancia = COALESCE(EXCLUDED.relevancia, products.relevancia),
              idcatalogo = COALESCE(EXCLUDED.idcatalogo, products.idcatalogo)
        RETURNING id
    """, (daterium_id, name, description, brand_id, family_id, thumb_url, image_url,
          supplier_name, supplier_cif, relevancia, idcatalogo))
    return cur.fetchone()[0]

def upsert_aecoc_category(cur, aecoc_id: str, name: str, parent_aecoc_id: Optional[str] = None) -> int:
    """Insertar categoría AECOC"""
    # Primero, obtener parent_id si existe
    parent_id = None
    if parent_aecoc_id:
        cur.execute("SELECT id FROM aecoc_categories WHERE aecoc_id = %s", (parent_aecoc_id,))
        row = cur.fetchone()
        if row:
            parent_id = row[0]
    
    # Calcular nivel basado en la longitud del código AECOC
    level = len(aecoc_id) // 2 if aecoc_id.isdigit() else 0
    
    cur.execute("""
        INSERT INTO aecoc_categories(aecoc_id, name, parent_id, level)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (aecoc_id) DO UPDATE
          SET name = EXCLUDED.name,
              parent_id = COALESCE(EXCLUDED.parent_id, aecoc_categories.parent_id),
              level = EXCLUDED.level
        RETURNING id
    """, (aecoc_id, name, parent_id, level))
    return cur.fetchone()[0]

def link_product_aecoc(cur, product_id: int, aecoc_category_id: int):
    """Relacionar producto con categoría AECOC"""
    cur.execute("""
        INSERT INTO product_aecoc(product_id, aecoc_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (product_id, aecoc_category_id))

def upsert_image(cur, product_id: int, url: str, is_primary: bool):
    cur.execute("""
        INSERT INTO product_images(product_id, url, is_primary)
        VALUES (%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (product_id, url, is_primary))

# ================== PARSING ESPECÍFICO DATERIUM ==================
def parse_aecoc_hierarchy(aecoc_elem) -> List[Dict[str, str]]:
    """Extrae la jerarquía de categorías AECOC"""
    categories = []
    if aecoc_elem is not None:
        ruta = aecoc_elem.find('.//ruta')
        if ruta is not None:
            for paso in ruta.findall('.//paso'):
                nombre_elem = paso.find('nombre')
                aecoc_id_elem = paso.find('aecocid')
                if nombre_elem is not None and aecoc_id_elem is not None:
                    categories.append({
                        'aecoc_id': aecoc_id_elem.text.strip(),
                        'name': nombre_elem.text.strip()
                    })
    return categories

def parse_float(txt: Optional[str]) -> Optional[float]:
    if not txt: return None
    try:
        return float(str(txt).replace(",", "."))
    except Exception:
        return None

def parse_and_upsert(conn: psycopg.Connection, xml_bytes: bytes) -> int:
    """Parse específico para la estructura XML real de Daterium"""
    root = etree.fromstring(xml_bytes)
    inserted = 0
    
    with conn.cursor() as cur:
        # Procesar cada ficha en <resultados>
        for ficha in root.xpath('.//ficha'):
            try:
                # Datos básicos del producto
                daterium_id = None
                id_elem = ficha.find('id')
                if id_elem is not None and id_elem.text:
                    try:
                        daterium_id = int(id_elem.text.strip())
                    except ValueError:
                        pass
                
                # Si no hay ID válido, usar idcatalogo
                if not daterium_id:
                    idcatalogo = ficha.get('idcatalogo')
                    if idcatalogo and idcatalogo.isdigit():
                        daterium_id = int(idcatalogo)
                
                # Campos obligatorios
                nombre = (ficha.findtext('nombre') or '').strip()
                if not nombre:
                    continue
                
                # Campos opcionales
                descripcion = ficha.findtext('descripcion') or ficha.findtext('descripcioncorta') or ''
                descripcion = descripcion.strip() if descripcion else None
                
                # Marca
                marca_elem = ficha.find('marca')
                marca_name = marca_elem.text.strip() if marca_elem is not None and marca_elem.text else None
                logo_marca = (ficha.findtext('logo_marca') or '').strip() or None
                
                # Familia y subfamilia
                familia_name = (ficha.findtext('familia') or '').strip() or None
                subfamilia_name = (ficha.findtext('subfamilia') or '').strip() or None
                
                # Proveedor
                supplier_name = (ficha.findtext('proveedor') or '').strip() or None
                supplier_cif = (ficha.findtext('proveedor_cif') or '').strip() or None
                
                # Relevancia
                relevancia = parse_float(ficha.get('relevancia'))
                idcatalogo_attr = ficha.get('idcatalogo')
                
                # Imágenes
                thumb = (ficha.findtext('thumb') or '').strip() or None
                img280 = (ficha.findtext('img280x240') or '').strip() or None
                img500 = (ficha.findtext('img500x500') or '').strip() or None
                amp = (ficha.findtext('amp') or '').strip() or None
                
                # Priorizar imagen de mejor calidad
                image_url = img500 or amp or img280 or thumb
                
                # Insertar marca
                brand_id = upsert_brand(cur, marca_name, logo_marca)
                
                # Insertar familias (jerarquía)
                parent_id = None
                if familia_name and familia_name.lower() != 'otros':
                    parent_id = upsert_family(cur, familia_name, None)
                
                family_id = None
                if subfamilia_name and subfamilia_name.lower() != 'otros':
                    family_id = upsert_family(cur, subfamilia_name, parent_id)
                else:
                    family_id = parent_id
                
                # Insertar producto
                pid = upsert_product(
                    cur=cur,
                    daterium_id=daterium_id,
                    name=nombre,
                    description=descripcion,
                    brand_id=brand_id,
                    family_id=family_id,
                    thumb_url=thumb,
                    image_url=image_url,
                    supplier_name=supplier_name,
                    supplier_cif=supplier_cif,
                    relevancia=relevancia,
                    idcatalogo=idcatalogo_attr
                )
                
                # Insertar imágenes
                if thumb:
                    upsert_image(cur, pid, thumb, image_url == thumb)
                if img280:
                    upsert_image(cur, pid, img280, image_url == img280)
                if img500:
                    upsert_image(cur, pid, img500, image_url == img500)
                if amp:
                    upsert_image(cur, pid, amp, image_url == amp)
                
                # Procesar categorías AECOC si existen
                aecoc_elem = ficha.find('aecoc')
                if aecoc_elem is not None:
                    categories = parse_aecoc_hierarchy(aecoc_elem)
                    parent_aecoc_id = None
                    
                    for cat in categories:
                        aecoc_cat_id = upsert_aecoc_category(
                            cur, cat['aecoc_id'], cat['name'], parent_aecoc_id
                        )
                        link_product_aecoc(cur, pid, aecoc_cat_id)
                        parent_aecoc_id = cat['aecoc_id']  # Para jerarquía
                
                inserted += 1
                
            except Exception as e:
                log_json(evt="product_error", error=str(e), producto=nombre if 'nombre' in locals() else 'unknown')
                continue
    
    return inserted

# ================== CURSOR Y ESTADO ==================
def ensure_cursor_table(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ingest_cursor (
          id          BIGSERIAL PRIMARY KEY,
          strategy    TEXT NOT NULL,
          cursor_key  TEXT NOT NULL,
          updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_cursor_strategy ON ingest_cursor(strategy);
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

# ================== HTTP CLIENT ==================
def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(connect=8.0, read=50.0, write=10.0, pool=100.0),
        headers={"User-Agent": "CompraInteligente/1.0", "Accept": "application/xml"},
    )

async def fetch_query(client: httpx.AsyncClient, query: str) -> Optional[bytes]:
    if not DATERIUM_USER_ID:
        raise SystemExit("Falta DATERIUM_USER_ID")
    
    # URL real según tu ejemplo
    url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(DATERIUM_USER_ID)}&searchbox={quote(query)}"
    
    try:
        r = await client.get(url)
        if r.status_code != 200:
            log_json(evt="http_error", query=query, status_code=r.status_code)
            return None
        return r.content
    except Exception as e:
        log_json(evt="http_exception", query=query, error=str(e))
        return None

# ================== GENERADORES DE BÚSQUEDA ==================
def gen_tool_terms() -> Iterable[str]:
    """Términos específicos de herramientas y ferretería"""
    tools = [
        # Herramientas eléctricas
        "taladro", "atornillador", "sierra", "calar", "radial", "amoladora",
        "lijadora", "fresadora", "router", "caladora", "ingletadora",
        
        # Herramientas manuales
        "llave", "destornillador", "alicate", "martillo", "nivel",
        "escuadra", "flexometro", "metro", "regla",
        
        # Accesorios
        "broca", "punta", "disco", "hoja", "mecha", "corona",
        "vaso", "dado", "extension", "carraca",
        
        # Fijación
        "tornillo", "tuerca", "arandela", "clavo", "taco", "anclaje",
        "remache", "espárrago", "tirafondo",
        
        # Marcas comunes
        "bosch", "makita", "dewalt", "milwaukee", "metabo", "festool",
        "stanley", "irwin", "wiha", "wera", "tivoly", "ruko"
    ]
    
    for term in tools:
        yield term
        # Variaciones con materiales
        for material in ["metal", "madera", "hormigon", "plastico"]:
            yield f"{term} {material}"

def gen_brands() -> Iterable[str]:
    """Marcas conocidas de herramientas"""
    brands = [
        "bosch", "makita", "dewalt", "milwaukee", "metabo", "festool",
        "stanley", "irwin", "wiha", "wera", "tivoly", "ruko", "asein",
        "bahco", "facom", "gedore", "knipex", "stabila", "fein"
    ]
    return brands

def gen_ngrams() -> Iterable[str]:
    """Generador de n-gramas alfabéticos"""
    alpha = "abcdefghijklmnopqrstuvwxyz"
    for c in alpha:
        yield c
    for a in alpha:
        for b in alpha:
            yield a + b

# ================== RUNNER PRINCIPAL ==================
async def run_strategy(strategy: str):
    if not DATERIUM_USER_ID:
        raise SystemExit("Falta DATERIUM_USER_ID")

    conn = db_conn()
    ensure_tables(conn)
    ensure_cursor_table(conn)

    # Generar términos según estrategia
    if strategy == "tools":
        keys = list(gen_tool_terms())
    elif strategy == "brands":
        keys = list(gen_brands())
    elif strategy == "ngrams":
        keys = list(gen_ngrams())
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
                    log_json(evt="batch_done", strategy=strategy, term=term, products=n)
                except Exception as e:
                    c2.rollback()
                    log_json(evt="batch_error", strategy=strategy, term=term, error=str(e))
            
            processed += 1
            set_cursor(conn, strategy, term)

    try:
        # Procesar en lotes
        for i in range(0, len(keys), 30):
            batch = keys[i:i+30]
            await asyncio.gather(*[worker(k) for k in batch])
            log_json(evt="progress", strategy=strategy, processed=processed, total=len(keys))
    finally:
        await client.aclose()
        conn.close()

# ================== MAIN ==================
def main():
    import argparse
    p = argparse.ArgumentParser(description="Ingestor Daterium Real → Postgres")
    p.add_argument("--mode", choices=["tools", "brands", "ngrams"], help="Estrategia")
    p.add_argument("--loop", action="store_true", help="Bucle infinito")
    args = p.parse_args()

    if args.loop:
        modes = ["brands", "tools", "ngrams"]
        while True:
            for mode in modes:
                try:
                    log_json(evt="cycle_start", mode=mode)
                    asyncio.run(run_strategy(mode))
                except Exception as e:
                    log_json(evt="cycle_error", mode=mode, error=str(e))
            log_json(evt="cycle_complete", sleep=600)
            time.sleep(600)  # 10 minutos entre ciclos
    else:
        if not args.mode:
            raise SystemExit("Usa --mode tools|brands|ngrams o --loop")
        log_json(evt="single_mode", mode=args.mode)
        asyncio.run(run_strategy(args.mode))

if __name__ == "__main__":
    main()