# app/admin.py
from __future__ import annotations
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
import psycopg

router = APIRouter(tags=["admin"])

def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("PGDATABASE_URL")
    if not dsn:
        raise HTTPException(status_code=500, detail="DATABASE_URL missing")
    return dsn

def _check_token(token: str):
    expected = os.getenv("MIGRATION_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

@router.get("/admin/debug_token_status")
def debug_token_status():
    """No expone el token. Solo indica si está presente y su longitud."""
    val = os.getenv("MIGRATION_TOKEN", "")
    return {"present": bool(val), "length": len(val)}

@router.post("/admin/migrate")
def run_migration(token: str = Query(..., description="Security token")):
    _check_token(token)

    sql_path = Path(__file__).resolve().parent.parent / "migrations" / "0002_catalog.sql"
    if not sql_path.exists():
        raise HTTPException(status_code=500, detail="migration file not found")

    sql_text = sql_path.read_text(encoding="utf-8")

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
    with psycopg.connect(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM brands");   brands = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM families"); families = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM products"); products = cur.fetchone()[0]
    return {"ok": True, "counts": {"brands": brands, "families": families, "products": products}}