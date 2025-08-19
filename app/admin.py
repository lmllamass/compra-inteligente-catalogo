# app/admin.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, List, Dict, Any

import psycopg
from fastapi import APIRouter, HTTPException, Query

# Opcionales para semilla desde Daterium
import httpx
from lxml import etree
from urllib.parse import quote

router = APIRouter(prefix="/admin", tags=["admin"])

# -------------------- helpers --------------------
def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("PGDATABASE_URL")
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL missing")
    return dsn

def _check_token(token: Optional[str]):
    expected = os.getenv("MIGRATION_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _load_sql_file(rel_path: str) -> str:
    """
    Carga SQL desde rel_path (relativo a la raíz del repo).
    Elimina líneas que empiezan por '#' (comentarios no válidos en Postgres).
    """
    sql_path = Path(__file__).resolve().parent.parent / rel_path
    if not sql_path.exists():
        raise HTTPException(status_code=500, detail=f"migration file not found: {rel_path}")
    lines = sql_path.read_text(encoding="utf-8").splitlines()
    return "\n".join(l for l in lines if not l.strip().startswith("#"))

def _http() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=5.0),
        headers={"User-Agent": "CompraInteligente/1.0", "Accept": "application/xml"},
    )

def _parse_float(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    try:
        return float(str(txt).replace(",", "."))
    except Exception:
        return None

# -------------------- endpoints base --------------------
@router.get("/debug_token_status")
def debug_token_status():
    """No expone el token. Solo indica si está presente y su longitud."""
    val = os.getenv("MIGRATION_TOKEN", "")
    return {"present": bool(val), "length": len(val)}

@router.post("/migrate")
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

@router.get("/tables")
def list_tables(token: str = Query(..., description="Security token")):
    _check_token(token)
    try:
        with psycopg.connect(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname='public'
                    ORDER BY tablename
                """)
                rows = [r[0] for r in cur.fetchall()]
        return {"ok": True, "tables": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"list failed: {e}")

@router.get("/count")
def count_tables(token: str = Query(..., description="Security token")):
    _check_token(token)
    try:
        with psycopg.connect(_dsn()) as conn:
            with conn.cursor() as cur:
                def count_of(tbl: str) -> Optional[int]:
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                        return cur.fetchone()[0]
                    except Exception:
                        return None

                counts = {
                    "brands":   count_of("brands"),
                    "families": count_of("families"),
                    "products": count_of("products"),
                }
        return {"ok": True, "counts": counts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"count failed: {e}")

# -------------------- semilla desde Daterium --------------------
SEED_QUERIES: List[str] = [
    "tivoly", "broca", "punta", "atornillado", "anclaje",
    "disco", "llave", "sierra", "adhesivo", "tornillo", "tuerca", "arandela"
]

def _upsert_brand(cur, name: Optional[str], logo_url: Optional[str]) -> Optional[int]:
    if not name:
        return None
    cur.execute("""
        INSERT INTO brands(name, logo_url)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE
          SET logo_url = COALESCE(EXCLUDED.logo_url, brands.logo_url)
        RETURNING id
    """, (name, logo_url))
    return cur.fetchone()[0]

def _upsert_family(cur, name: Optional[str], parent_id: Optional[int] = None) -> Optional[int]:
    if not name:
        return None
    cur.execute("""
        INSERT INTO families(name, parent_id)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE
          SET parent_id = COALESCE(EXCLUDED.parent_id, families.parent_id)
        RETURNING id
    """, (name, parent_id))
    return cur.fetchone()[0]

def _upsert_product(
    cur,
    daterium_id: Optional[int],
    name: str,
    description: Optional[str],
    brand_id: Optional[int],
    family_id: Optional[int],
    ean: Optional[str],
    sku: Optional[str],
    pvp: Optional[float],
    thumb_url: Optional[str],
    image_url: Optional[str],
) -> int:
    cur.execute("""
        INSERT INTO products(daterium_id, name, description, brand_id, family_id, ean, sku, pvp, thumb_url, image_url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (daterium_id) DO UPDATE
          SET name        = EXCLUDED.name,
              description = COALESCE(EXCLUDED.description, products.description),
              brand_id    = COALESCE(EXCLUDED.brand_id, products.brand_id),
              family_id   = COALESCE(EXCLUDED.family_id, products.family_id),
              ean         = COALESCE(EXCLUDED.ean, products.ean),
              sku         = COALESCE(EXCLUDED.sku, products.sku),
              pvp         = COALESCE(EXCLUDED.pvp, products.pvp),
              thumb_url   = COALESCE(EXCLUDED.thumb_url, products.thumb_url),
              image_url   = COALESCE(EXCLUDED.image_url, products.image_url)
        RETURNING id
    """, (daterium_id, name, description, brand_id, family_id, ean, sku, pvp, thumb_url, image_url))
    return cur.fetchone()[0]

def _upsert_image(cur, product_id: int, url: str, is_primary: bool):
    cur.execute("""
        INSERT INTO product_images(product_id, url, is_primary)
        VALUES (%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (product_id, url, is_primary))

@router.post("/seed_basic")
def seed_basic(
    token: str = Query(..., description="Security token"),
    queries: Optional[str] = Query(None, description="CSV de términos. Si no, usa los de SEED_QUERIES"),
):
    """
    Carga básica: lanza búsquedas en Daterium y persiste marcas/familias/productos.
    - token: MIGRATION_TOKEN
    - queries (opcional): 'tivoly,broca,disco'
    """
    _check_token(token)
    user_id = os.getenv("DATERIUM_USER_ID", "").strip()
    if not user_id:
        raise HTTPException(status_code=500, detail="Falta DATERIUM_USER_ID")

    q_list = [q.strip() for q in (queries.split(",") if queries else SEED_QUERIES) if q.strip()]
    total = 0

    with psycopg.connect(_dsn(), autocommit=False) as conn, conn.cursor() as cur:
        with _http() as c:
            for q in q_list:
                url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(user_id)}&searchbox={quote(q)}"
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

                    ean = None
                    pvp = None
                    ref = ficha.find(".//referencias/referencia")
                    if ref is not None:
                        ean = (ref.findtext("ean") or "").strip() or None
                        pvp = _parse_float(ref.findtext("pvp"))

                    brand_id = _upsert_brand(cur, marca_name, logo_marca)

                    parent_id = _upsert_family(cur, familia_name, None) if familia_name else None
                    family_id = _upsert_family(cur, subfamilia_name, parent_id) if subfamilia_name else parent_id

                    pid = _upsert_product(cur, daterium_id, nombre, descripcion, brand_id, family_id, ean, None, pvp, thumb, image_url)

                    if thumb:
                        _upsert_image(cur, pid, thumb, image_url == thumb)
                    if img280:
                        _upsert_image(cur, pid, img280, image_url == img280)
                    if img500:
                        _upsert_image(cur, pid, img500, image_url == img500)

                    total += 1

        conn.commit()

    return {"ok": True, "inserted_or_updated": total, "queries": q_list}

# -------------------- monitorización de ingesta --------------------
@router.get("/ingest_stats")
def ingest_stats(token: str = Query(..., description="Security token")):
    _check_token(token)
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT strategy, cursor_key, updated_at
            FROM ingest_cursor
            ORDER BY updated_at DESC
        """)
        rows = cur.fetchall()
    return {
        "ok": True,
        "stats": [
            {"strategy": r[0], "cursor_key": r[1], "updated_at": r[2].isoformat()}
            for r in rows
        ],
    }

@router.get("/recent")
def recent_products(
    token: str = Query(..., description="Security token"),
    limit: int = Query(20, ge=1, le=200, description="cuántos items devolver"),
):
    _check_token(token)
    sql = """
    SELECT
      p.id, p.daterium_id, p.name, p.description, p.ean, p.pvp, p.thumb_url, p.image_url,
      b.name AS brand, f.name AS subfamily, pf.name AS family
    FROM products p
    LEFT JOIN brands b   ON b.id = p.brand_id
    LEFT JOIN families f ON f.id = p.family_id
    LEFT JOIN families pf ON pf.id = f.parent_id
    ORDER BY p.id DESC
    LIMIT %s
    """
    try:
        with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for (pid, did, name, desc, ean, pvp, thumb, img, brand, subfamily, family) in rows:
            out.append({
                "id": did or pid,
                "internal_id": pid,
                "nombre": name,
                "marca": brand,
                "familia": family,
                "subfamilia": subfamily,
                "ean": ean,
                "pvp": float(pvp) if pvp is not None else None,
                "img": img or thumb,
            })
        return {"ok": True, "items": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"recent failed: {e}")

@router.get("/ingest_log")
def ingest_log(
    token: str = Query(..., description="Security token"),
    limit: int = Query(50, ge=1, le=500),
):
    _check_token(token)
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.ingest_log') IS NOT NULL;")
        if not cur.fetchone()[0]:
            return {"ok": False, "reason": "table_not_found"}
        cur.execute("""
            SELECT ts, strategy, item_key, status, note
            FROM ingest_log
            ORDER BY ts DESC
            LIMIT %s
        """, (limit,))
        rows = [{"ts": str(ts), "strategy": s, "item_key": k, "status": st, "note": n}
                for (ts, s, k, st, n) in cur.fetchall()]
    return {"ok": True, "rows": rows, "limit": limit}

@router.get("/progress")
def progress(token: str = Query(..., description="Security token")):
    _check_token(token)
    out: Dict[str, Any] = {"ok": True, "counts": {}, "latest": {}}
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        # counts
        for t in ("brands", "families", "products"):
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                out["counts"][t] = cur.fetchone()[0]
            except Exception:
                out["counts"][t] = None

        # latest row in ingest_log
        cur.execute("SELECT to_regclass('public.ingest_log') IS NOT NULL;")
        if cur.fetchone()[0]:
            cur.execute("""
                SELECT ts, strategy, item_key, status, note
                FROM ingest_log
                ORDER BY ts DESC
                LIMIT 1
            """)
            r = cur.fetchone()
            if r:
                ts, s, k, st, n = r
                out["latest"] = {
                    "ts": str(ts),
                    "strategy": s,
                    "item_key": k,
                    "status": st,
                    "note": n,
                }
        else:
            out["latest"] = {"info": "ingest_log no existe"}
    return out