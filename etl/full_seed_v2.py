# etl/full_seed_v2.py
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

# ================== NUEVA TABLA PARA EANS ==================
def ensure_ean_table(conn: psycopg.Connection):
    """Crear tabla para múltiples EANs por producto"""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_eans (
          id             BIGSERIAL PRIMARY KEY,
          product_id     BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
          ean            TEXT NOT NULL,
          packaging_type TEXT,                    -- 'unit', 'pack', 'box', 'pallet', etc.
          quantity       INTEGER DEFAULT 1,      -- cantidad en el packaging
          is_primary     BOOLEAN DEFAULT FALSE,  -- EAN principal (unidad)
          created_at     TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_product_eans_product ON product_eans(product_id);
        CREATE INDEX IF NOT EXISTS idx_product_eans_ean ON product_eans(ean);
        CREATE UNIQUE INDEX IF NOT EXISTS ux_product_eans_product_ean 
          ON product_eans(product_id, ean);
        """)
    conn.commit()

# ================== LOG ==================
def log(msg: str):
    print(msg, flush=True, file=sys.stdout)

def log_json(**kv):
    log(json.dumps(kv, ensure_ascii=False))

# ================== DB HELPERS ACTUALIZADOS ==================
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
                   sku: Optional[str],
                   pvp: Optional[float],
                   thumb_url: Optional[str],
                   image_url: Optional[str]) -> int:
    """Producto SIN EAN directo - ahora los EANs van en tabla separada"""
    cur.execute("""
        INSERT INTO products(daterium_id, name, description, brand_id, family_id, sku, pvp, thumb_url, image_url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (daterium_id) DO UPDATE
          SET name = EXCLUDED.name,
              description = COALESCE(EXCLUDED.description, products.description),
              brand_id = COALESCE(EXCLUDED.brand_id, products.brand_id),
              family_id = COALESCE(EXCLUDED.family_id, products.family_id),
              sku = COALESCE(EXCLUDED.sku, products.sku),
              pvp = COALESCE(EXCLUDED.pvp, products.pvp),
              thumb_url = COALESCE(EXCLUDED.thumb_url, products.thumb_url),
              image_url = COALESCE(EXCLUDED.image_url, products.image_url)
        RETURNING id
    """, (daterium_id, name, description, brand_id, family_id, sku, pvp, thumb_url, image_url))
    return cur.fetchone()[0]

def upsert_product_ean(cur, product_id: int, ean: str, packaging_type: Optional[str] = None, 
                      quantity: int = 1, is_primary: bool = False):
    """Insertar EAN con información de packaging"""
    if not ean or not ean.strip():
        return
    
    cur.execute("""
        INSERT INTO product_eans(product_id, ean, packaging_type, quantity, is_primary)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_id, ean) DO UPDATE
          SET packaging_type = COALESCE(EXCLUDED.packaging_type, product_eans.packaging_type),
              quantity = COALESCE(EXCLUDED.quantity, product_eans.quantity),
              is_primary = EXCLUDED.is_primary OR product_eans.is_primary
    """, (product_id, ean.strip(), packaging_type, quantity, is_primary))

def upsert_image(cur, product_id: int, url: str, is_primary: bool):
    cur.execute("""
        INSERT INTO product_images(product_id, url, is_primary)
        VALUES (%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (product_id, url, is_primary))

# ================== PARSING MEJORADO ==================
def detect_packaging_type(ref_data: Dict[str, Any]) -> tuple[str, int]:
    """
    Detecta el tipo de packaging y cantidad basado en los datos de la referencia
    Returns: (packaging_type, quantity)
    """
    # Buscar indicadores en diferentes campos
    ref_text = str(ref_data.get('ref', '')).lower()
    descripcion = str(ref_data.get('descripcion', '')).lower()
    cantidad_txt = str(ref_data.get('cantidad', '')).lower()
    envase_txt = str(ref_data.get('envase', '')).lower()
    
    # Combinar todos los textos para análisis
    full_text = f"{ref_text} {descripcion} {cantidad_txt} {envase_txt}".lower()
    
    # Patrones de packaging conocidos
    if any(word in full_text for word in ['unidad', 'unit', 'ud', 'individual', 'single']):
        return 'unit', 1
    elif any(word in full_text for word in ['pack', 'blister', 'blist']):
        # Buscar cantidad en pack
        import re
        pack_match = re.search(r'pack\s*(\d+)|(\d+)\s*pack|blister\s*(\d+)|(\d+)\s*blister', full_text)
        if pack_match:
            qty = next(int(g) for g in pack_match.groups() if g and g.isdigit())
            return 'pack', qty
        return 'pack', 1
    elif any(word in full_text for word in ['caja', 'box', 'carton']):
        # Buscar cantidad en caja
        import re
        box_match = re.search(r'caja\s*(\d+)|(\d+)\s*caja|box\s*(\d+)|(\d+)\s*box', full_text)
        if box_match:
            qty = next(int(g) for g in box_match.groups() if g and g.isdigit())
            return 'box', qty
        return 'box', 1
    elif any(word in full_text for word in ['pallet', 'pale']):
        return 'pallet', 1
    else:
        # Por defecto asumir unidad si no hay indicadores claros
        return 'unit', 1

def parse_float(txt: Optional[str]) -> Optional[float]:
    if not txt: return None
    try:
        return float(str(txt).replace(",", "."))
    except Exception:
        return None

def extract_references_enhanced(ficha_elem) -> List[Dict[str, Any]]:
    """
    Extrae todas las referencias de un producto con información detallada
    Maneja múltiples estructuras XML de Daterium
    """
    references = []
    
    # Estructura 1: <referencias><referencia>...</referencia></referencias>
    referencias_elem = ficha_elem.find('.//referencias')
    if referencias_elem is not None:
        for ref in referencias_elem.findall('.//referencia'):
            ref_data = {}
            for child in ref:
                ref_data[child.tag] = child.text
            if ref_data:
                references.append(ref_data)
    
    # Estructura 2: <referencia> directa bajo <ficha>
    for ref in ficha_elem.findall('.//referencia'):
        if