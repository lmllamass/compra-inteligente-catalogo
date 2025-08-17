# etl/migrate.py
# Ejecuta migrations/0002_catalog.sql contra la DB usando la URL privada de Railway
# Usa psycopg3 y una transacción atómica.
from __future__ import annotations
import os
import sys
from pathlib import Path

import psycopg
from psycopg import sql

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
MIGRATION_FILE = MIGRATIONS_DIR / "0002_catalog.sql"

def _resolve_dsn() -> str:
    """
    Resuelve la cadena de conexión priorizando el endpoint PRIVADO de Railway.
    - DATABASE_URL normalmente apunta a *.railway.internal dentro de Railway (privado).
    - Si no existe (por ejemplo, ejecutas local), intenta DATABASE_PUBLIC_URL.
    """
    dsn = os.getenv("DATABASE_URL") or os.getenv("PGDATABASE_URL")
    if dsn:
        return dsn

    dsn_public = os.getenv("DATABASE_PUBLIC_URL")
    if dsn_public:
        print("[WARN] Usando DATABASE_PUBLIC_URL (esto puede incurrir en egress).", file=sys.stderr)
        return dsn_public

    # Construcción manual como último recurso (si Railway expone PG* vars)
    host = os.getenv("PGHOST")
    user = os.getenv("PGUSER")
    pwd  = os.getenv("PGPASSWORD")
    db   = os.getenv("PGDATABASE", "railway")
    port = os.getenv("PGPORT", "5432")
    if all([host, user, pwd]):
        return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

    raise RuntimeError("No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL ni variables PGHOST/PGUSER/PGPASSWORD.")

def main():
    if not MIGRATION_FILE.exists():
        print(f"[ERROR] No existe el archivo de migración: {MIGRATION_FILE}", file=sys.stderr)
        sys.exit(1)

    dsn = _resolve_dsn()
    print(f"[INFO] Conectando a DB…")
    # autocommit=False para que sea transaccional
    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            sql_text = MIGRATION_FILE.read_text(encoding="utf-8")
            print(f"[INFO] Ejecutando {MIGRATION_FILE.name}…")
            cur.execute(sql.SQL(sql_text))
        conn.commit()
    print("[OK] Migración aplicada correctamente.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(2)