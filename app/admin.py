# app/admin.py
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional, List, Dict, Any
import time
import unicodedata

import psycopg
from fastapi import APIRouter, HTTPException, Query

# Opcionales para semilla desde Daterium
import httpx
from lxml import etree
from urllib.parse import quote

router = APIRouter(prefix="/admin", tags=["admin"])

# ======================================================
# Helpers conexión
# ======================================================
def _dsn() -> str:
    """
    Elige el DSN correcto y fija sslmode según el tipo de endpoint.
    - Privado (railway.internal / IP RFC4193): sslmode=disable
    - Público (tcp.railway.app / IP pública): sslmode=require
    Prioridad: DATABASE_URL > PGDATABASE_URL > DATABASE_PUBLIC_URL
    """
    import re
    for key in ("DATABASE_URL", "PGDATABASE_URL", "DATABASE_PUBLIC_URL"):
        val = os.getenv(key)
        if not val:
            continue

        sep = "&" if "?" in val else "?"

        host = None
        try:
            host = re.search(r'@([^/:]+)', val).group(1)
        except Exception:
            pass

        is_private = False
        if host:
            if ".railway.internal" in host:
                is_private = True
            if ":" in host and host.lower().startswith("fd"):  # IPv6 ULA
                is_private = True

        if is_private:
            if "sslmode=" not in val:
                val = f"{val}{sep}sslmode=disable"
        else:
            if "sslmode=" not in val:
                val = f"{val}{sep}sslmode=require"

        return val

    raise HTTPException(status_code=500, detail="No DATABASE_URL/PGDATABASE_URL/DATABASE_PUBLIC_URL set")


def _connect():
    dsn = _dsn()
    from time import sleep
    last_err = None
    for _ in range(3):
        try:
            return psycopg.connect(
                dsn,
                connect_timeout=8,
                options="-c statement_timeout=30000 -c idle_in_transaction_session_timeout=30000"
            )
        except Exception as e:
            last_err = e
            sleep(1.0)
    raise HTTPException(status_code=500, detail=f"DB connect failed: {last_err}")


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

# ======================================================
# EAN helpers (robustos)
# ======================================================
_EAN_CAND_RE = re.compile(r"\b(\d{8}|\d{12,14})\b")  # EAN8 / GTIN-12/13/14

def _gtin_checksum_ok(code: str) -> bool:
    """Valida EAN/GTIN de 8/12/13/14 dígitos (checksum módulo 10)."""
    n = len(code)
    if n not in (8, 12, 13, 14) or not code.isdigit():
        return False
    s = 0
    for i, c in enumerate(reversed(code[:-1]), start=1):
        w = 3 if i % 2 == 1 else 1
        s += int(c) * w
    check = (10 - (s % 10)) % 10
    return check == int(code[-1])

def _extract_eans_from_ficha(ficha) -> list[str]:
    """Devuelve lista de EAN/GTIN candidatos de <referencias> y textos, validados y priorizados."""
    eans: list[str] = []

    # 1) explícitos en referencias
    for ref in ficha.xpath(".//referencias/referencia"):
        for tag in ("ean", "ean13", "gtin", "codigo_barras"):
            val = (ref.findtext(tag) or "").strip()
            if val and val.isdigit():
                eans.append(val)
        # dentro de textos libres tipo sku/codigo
        for tag in ("sku", "codigo", "ref", "referencia"):
            val = (ref.findtext(tag) or "").strip()
            if val:
                eans.extend(_EAN_CAND_RE.findall(val))

    # 2) buscar en nombre/descripcion si hiciera falta
    for tag in ("nombre", "descripcion", "descripcioncorta"):
        txt = (ficha.findtext(tag) or "").strip()
        if txt:
            eans.extend(_EAN_CAND_RE.findall(txt))

    # Normaliza, valida checksum y prioriza GTIN-13
    uniq: list[str] = []
    for e in eans:
        e = e.strip()
        if e not in uniq and _gtin_checksum_ok(e):
            uniq.append(e)

    uniq.sort(key=lambda x: (len(x) != 13, len(x)))  # primero 13 dígitos
    return uniq

# ======================================================
# Endpoints base
# ======================================================
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

# ======================================================
# Semilla desde Daterium (con EAN robusto)
# ======================================================
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
                    # ids
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

                    # pvp: primero que venga
                    pvp = None
                    ref = ficha.find(".//referencias/referencia")
                    if ref is not None:
                        pvp = _parse_float(ref.findtext("pvp"))

                    # EAN robusto (validado y priorizado GTIN-13)
                    ean_list = _extract_eans_from_ficha(ficha)
                    ean = ean_list[0] if ean_list else None

                    brand_id = _upsert_brand(cur, marca_name, logo_marca)
                    parent_id = _upsert_family(cur, familia_name, None) if familia_name else None
                    family_id = _upsert_family(cur, subfamilia_name, parent_id) if subfamilia_name else parent_id

                    pid = _upsert_product(
                        cur, daterium_id, nombre, descripcion, brand_id, family_id,
                        ean, None, pvp, thumb, image_url
                    )

                    if thumb:  _upsert_image(cur, pid, thumb,  image_url == thumb)
                    if img280: _upsert_image(cur, pid, img280, image_url == img280)
                    if img500: _upsert_image(cur, pid, img500, image_url == img500)

                    total += 1

        conn.commit()

    return {"ok": True, "inserted_or_updated": total, "queries": q_list}

# ======================================================
# Monitorización de ingesta
# ======================================================
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
        for t in ("brands", "families", "products"):
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                out["counts"][t] = cur.fetchone()[0]
            except Exception:
                out["counts"][t] = None

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


@router.get("/debug_env")
def debug_env(token: str = Query(...)):
    _check_token(token)
    keys = ["DATABASE_URL", "PGDATABASE_URL", "DATABASE_PUBLIC_URL"]
    present = {k: bool(os.getenv(k)) for k in keys}
    dsn_status = "ok"
    dsn_val = None
    try:
        dsn_val = _dsn()
    except Exception as e:
        dsn_status = f"error: {e}"
    return {"ok": True, "present": present, "dsn_status": dsn_status, "using": dsn_val is not None}


@router.get("/debug_sql")
def debug_sql(token: str = Query(...)):
    _check_token(token)
    try:
        with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
            cur.execute("SELECT version(), now()")
            ver, now = cur.fetchone()
        return {"ok": True, "version": ver, "now": str(now)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/progress_safe")
def progress_safe(token: str = Query(...)):
    _check_token(token)
    out = {"ok": True, "counts": {}, "latest": {}, "errors": []}
    try:
        with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
            for t in ("brands", "families", "products"):
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    out["counts"][t] = cur.fetchone()[0]
                except Exception as e:
                    out["counts"][t] = None
                    out["errors"].append(f"count {t}: {e}")

            try:
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
                            "ts": str(ts), "strategy": s, "item_key": k,
                            "status": st, "note": n
                        }
                else:
                    out["latest"] = {"info": "ingest_log no existe"}
            except Exception as e:
                out["errors"].append(f"latest ingest_log: {e}")
    except Exception as e:
        return {"ok": False, "fatal": str(e)}

    return out


# ======================================================
# Backfill EAN vía endpoint (opcional)
# ======================================================
@router.post("/backfill_ean")
def backfill_ean(
    token: str = Query(...),
    limit: int = Query(500, ge=1, le=5000),
    sleep: float = Query(0.2, ge=0, le=2.0)
):
    """
    Completa EAN para productos sin EAN consultando Daterium por daterium_id.
    """
    _check_token(token)
    user_id = os.getenv("DATERIUM_USER_ID","").strip()
    if not user_id:
        raise HTTPException(500, "Falta DATERIUM_USER_ID")

    done = 0
    import time
    with psycopg.connect(_dsn(), autocommit=False) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, daterium_id FROM products
            WHERE ean IS NULL AND daterium_id IS NOT NULL
            ORDER BY id ASC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

        with _http() as c:
            for pid, did in rows:
                time.sleep(sleep)
                url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(user_id)}&searchbox={quote(str(did))}"
                r = c.get(url)
                if r.status_code != 200:
                    continue
                root = etree.fromstring(r.content)
                ficha = root.find(".//ficha")
                if ficha is None:
                    continue
                eans = _extract_eans_from_ficha(ficha)
                if not eans:
                    continue
                ean = eans[0]
                cur.execute("UPDATE products SET ean = %s WHERE id = %s", (ean, pid))
                done += 1

        conn.commit()

    return {"ok": True, "updated": done, "limit": limit}
# --- Backfill EAN en tandas desde la API ---

def _normalize_ean(txt: str | None) -> str | None:
    if not txt: return None
    digits = re.sub(r"[^\d]", "", txt)
    return digits if len(digits) in (8, 12, 13, 14) else None

def _prefer_ean(eans: list[str]) -> str | None:
    if not eans: return None
    for L in (13, 14, 12, 8):
        cand = [x for x in eans if len(x) == L]
        if cand: return cand[0]
    return eans[0]

def _extract_eans(xml_bytes: bytes) -> list[str]:
    root = etree.fromstring(xml_bytes)
    out: list[str] = []
    for e in root.xpath(".//referencias/referencia/ean"):
        v = _normalize_ean((e.text or "").strip())
        if v and v not in out: out.append(v)
    if not out:
        for e in root.xpath(".//ean"):
            v = _normalize_ean((e.text or "").strip())
            if v and v not in out: out.append(v)
    return out

@router.post("/backfill_ean_batch")
def backfill_ean_batch(
    token: str = Query(..., description="Security token"),
    limit: int = Query(500, ge=1, le=5000),
    pause_ms: int = Query(150, ge=0, le=5000, description="Pausa entre llamadas"),
    dry: bool = Query(False, description="Dry-run (no escribe)"),
):
    """
    Rellena EAN para productos sin EAN en tandas.
    - Busca 'limit' productos con ean IS NULL
    - Para cada uno consulta Daterium por 'daterium_id'
    - Si encuentra EAN válido, lo actualiza
    """
    _check_token(token)
    user_id = os.getenv("DATERIUM_USER_ID", "").strip()
    if not user_id:
        raise HTTPException(status_code=500, detail="Falta DATERIUM_USER_ID")

    updated = 0
    scanned = 0
    tried = 0
    errors = 0
    details: list[dict] = []

    with psycopg.connect(_dsn(), autocommit=False) as conn, conn.cursor() as cur:
        # lote de candidatos
        cur.execute(
            "SELECT id, daterium_id FROM products WHERE ean IS NULL AND daterium_id IS NOT NULL ORDER BY id ASC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()

        if not rows:
            return {"ok": True, "done": True, "message": "No hay más productos sin EAN."}

        with _http() as c:
            for pid, did in rows:
                scanned += 1
                try:
                    url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(user_id)}&searchbox={quote(str(did))}"
                    r = c.get(url)
                    tried += 1
                    if r.status_code != 200 or not r.content:
                        details.append({"id": pid, "did": did, "status": "no-content"})
                    else:
                        eans = _extract_eans(r.content)
                        final = _prefer_ean(eans)
                        if final and not dry:
                            cur.execute("UPDATE products SET ean=%s WHERE id=%s", (final, pid))
                            updated += 1
                            details.append({"id": pid, "did": did, "ean": final, "status": "updated"})
                        else:
                            details.append({"id": pid, "did": did, "status": "no-ean"})
                except Exception as e:
                    errors += 1
                    details.append({"id": pid, "did": did, "error": str(e)})
                if pause_ms > 0:
                    time.sleep(pause_ms / 1000.0)

        if dry:
            conn.rollback()
        else:
            conn.commit()

    # devolvemos resumen (capamos details a 50 para evitar payloads enormes)
    return {
        "ok": True,
        "dry": dry,
        "limit": limit,
        "scanned": scanned,
        "tried": tried,
        "updated": updated,
        "errors": errors,
        "details": details[:50],
        "more": scanned == limit  # si true, probablemente quedan más por procesar
    }
# ===================== IMPORTACIÓN POR MARCA (mínima) =====================


# -- Helpers ya existentes que reutilizamos si están definidos:
# _dsn(), _check_token(token), _upsert_brand(cur, name, logo_url)

def _http_xml() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=10.0),
        headers={"User-Agent": "CompraInteligente/1.0", "Accept": "application/xml"},
    )

def clean_ean(raw: Optional[str]) -> Optional[str]:
    """
    Normaliza y valida EAN:
    - Mantiene solo dígitos
    - Si 13 dígitos: valida checksum (mód 10). Si ok, lo devuelve.
    - Si 12 u 8: intenta calcular dígito de control y devolver EAN-13/8 válido.
    - En otro caso: None
    """
    if not raw:
        return None
    s = re.sub(r"\D+", "", raw)
    if len(s) == 13:
        # validar checksum
        digits = [int(c) for c in s]
        check = digits[-1]
        body = digits[:-1]
        total = sum((d if i % 2 == 0 else d * 3) for i, d in enumerate(body))
        calc = (10 - (total % 10)) % 10
        return s if calc == check else None
    if len(s) == 12:
        digits = [int(c) for c in s]
        total = sum((d if i % 2 == 0 else d * 3) for i, d in enumerate(digits))
        check = (10 - (total % 10)) % 10
        return s + str(check)
    if len(s) == 8:
        # EAN-8: validar/calc checksum
        digits = [int(c) for c in s[:-1]]
        check = int(s[-1])
        total = (digits[0]*3 + digits[1]*1 + digits[2]*3 + digits[3]*1 +
                 digits[4]*3 + digits[5]*1 + digits[6]*3)
        calc = (10 - (total % 10)) % 10
        return s if calc == check else None
    return None

def norm_text(txt: Optional[str]) -> Optional[str]:
    if not txt:
        return None
    # Normaliza y quita rarezas de espacios
    t = unicodedata.normalize("NFC", txt).strip()
    return re.sub(r"\s+", " ", t)

def fetch_daterium_by_query(user_id: str, query: str) -> List[Dict[str, Any]]:
    """
    Llama al endpoint XML de Daterium con el texto dado y devuelve
    una lista de dicts con los campos mínimos.
    """
    url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(user_id)}&searchbox={quote(query)}"
    out: List[Dict[str, Any]] = []
    with _http_xml() as c:
        r = c.get(url)
        if r.status_code != 200:
            return out
        root = etree.fromstring(r.content)

    for ficha in root.xpath(".//ficha"):
        id_txt = ficha.findtext("id")
        idcat  = ficha.get("idcatalogo")
        daterium_id = None
        for candidate in (id_txt, idcat):
            if candidate and str(candidate).strip().isdigit():
                daterium_id = int(str(candidate).strip())
                break
        if not daterium_id:
            continue

        nombre = norm_text(ficha.findtext("nombre") or "")
        if not nombre:
            continue

        # imágenes mínimas
        thumb = norm_text(ficha.findtext("thumb") or "")
        img280 = norm_text(ficha.findtext("img280x240") or "")
        img500 = norm_text(ficha.findtext("img500x500") or "")
        image_url = img500 or img280 or thumb or None
        thumb_url = thumb or img280 or None

        # EAN
        ean = None
        ref = ficha.find(".//referencias/referencia")
        if ref is not None:
            ean = clean_ean(ref.findtext("ean"))

        out.append({
            "daterium_id": daterium_id,
            "name": nombre,
            "ean": ean,
            "thumb_url": thumb_url,
            "image_url": image_url,
        })
    return out

def _brand_id_by_name(cur, brand_name: str) -> Optional[int]:
    cur.execute("SELECT id FROM brands WHERE name = %s", (brand_name,))
    row = cur.fetchone()
    return row[0] if row else None

def _delete_brand_products(cur, brand_id: int) -> int:
    # Eliminar imágenes de los productos de esa marca
    cur.execute("""
        DELETE FROM product_images
        WHERE product_id IN (SELECT id FROM products WHERE brand_id = %s)
    """, (brand_id,))
    # Eliminar productos
    cur.execute("DELETE FROM products WHERE brand_id = %s RETURNING id", (brand_id,))
    deleted = cur.rowcount or 0
    return deleted

@router.post("/import_brand")
def import_brand(
    token: str = Query(..., description="Security token"),
    brand: str = Query(..., description="Nombre exacto de la marca (tal como está en DB o en Daterium)"),
    pause_ms: int = Query(150, ge=0, le=5000, description="Pausa tras la petición a Daterium"),
    dry: bool = Query(False, description="Si true, no escribe cambios"),
):
    """
    Importa productos por *marca* desde Daterium:
    - Busca/crea brand
    - Borra todo lo existente de esa marca
    - Inserta productos mínimos: daterium_id, name, ean, thumb_url, image_url, brand_id
    """
    _check_token(token)
    user_id = os.getenv("DATERIUM_USER_ID", "").strip()
    if not user_id:
        raise HTTPException(status_code=500, detail="Falta DATERIUM_USER_ID")

    brand_name = norm_text(brand)
    if not brand_name:
        raise HTTPException(status_code=400, detail="brand vacío")

    try:
        with psycopg.connect(_dsn(), autocommit=False) as conn, conn.cursor() as cur:
            # upsert brand con logo null (mínimo)
            brand_id = _brand_id_by_name(cur, brand_name)
            if not brand_id:
                brand_id = _upsert_brand(cur, brand_name, None)

            # descarga
            items = fetch_daterium_by_query(user_id, brand_name)
            if pause_ms:
                time.sleep(pause_ms / 1000.0)

            # borrar lo anterior
            deleted = 0
            if not dry:
                deleted = _delete_brand_products(cur, brand_id)

            inserted = 0
            for it in items:
                did = it["daterium_id"]
                name = it["name"]
                ean = it["ean"]
                thumb = it["thumb_url"]
                image = it["image_url"]

                sql = """
                INSERT INTO products(daterium_id, name, brand_id, ean, thumb_url, image_url)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (daterium_id) DO UPDATE
                  SET name = EXCLUDED.name,
                      brand_id = EXCLUDED.brand_id,
                      ean = COALESCE(EXCLUDED.ean, products.ean),
                      thumb_url = COALESCE(EXCLUDED.thumb_url, products.thumb_url),
                      image_url = COALESCE(EXCLUDED.image_url, products.image_url)
                RETURNING id
                """
                if not dry:
                    cur.execute(sql, (did, name, brand_id, ean, thumb, image))
                inserted += 1

            if dry:
                conn.rollback()
            else:
                conn.commit()

        return {
            "ok": True,
            "brand": brand_name,
            "brand_id": brand_id,
            "deleted_prev": deleted if not dry else 0,
            "fetched": len(items),
            "inserted_or_updated": inserted,
            "dry": dry,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"import_brand failed: {e}")

@router.post("/import_all_brands")
def import_all_brands(
    token: str = Query(...),
    batch: int = Query(5, ge=1, le=100),
    pause_ms: int = Query(300, ge=0, le=5000),
    dry: bool = Query(False),
):
    """
    Recorre todas las marcas de la tabla brands y llama a importación por cada una.
    Útil cuando ya tienes brands pobladas (de tu semilla).
    """
    _check_token(token)
    user_id = os.getenv("DATERIUM_USER_ID", "").strip()
    if not user_id:
        raise HTTPException(status_code=500, detail="Falta DATERIUM_USER_ID")

    brands: List[str] = []
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute("SELECT name FROM brands ORDER BY id ASC")
        brands = [r[0] for r in cur.fetchall()]

    results = []
    processed = 0
    for name in brands:
        r = import_brand(token=token, brand=name, pause_ms=pause_ms, dry=dry)  # reutilizamos endpoint
        results.append({"brand": name, "fetched": r["fetched"], "inserted_or_updated": r["inserted_or_updated"]})
        processed += 1
        if processed % batch == 0 and pause_ms:
            time.sleep(pause_ms / 1000.0)

    return {"ok": True, "count_brands": len(brands), "results": results, "dry": dry}

@router.get("/brand_status")
def brand_status(
    token: str = Query(...),
    brand: str = Query(..., description="Nombre exacto de marca"),
):
    _check_token(token)
    brand_name = norm_text(brand)
    if not brand_name:
        raise HTTPException(status_code=400, detail="brand vacío")

    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM brands WHERE name = %s", (brand_name,))
        row = cur.fetchone()
        if not row:
            return {"ok": True, "brand": brand_name, "exists": False}
        bid = row[0]
        cur.execute("SELECT COUNT(*) FROM products WHERE brand_id = %s", (bid,))
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM products WHERE brand_id = %s AND ean IS NOT NULL", (bid,))
        with_ean = cur.fetchone()[0]
        return {"ok": True, "brand": brand_name, "exists": True, "products": total, "with_ean": with_ean}