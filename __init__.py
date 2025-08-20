mkdir -p etl
touch etl/__init__.py

# Crea el script de reparación
cat > etl/fix_encoding.py <<'PY'
from __future__ import annotations
import os, re, sys, json
from typing import Optional, Tuple
import psycopg

UNICODE_ESCAPE_RE = re.compile(r'\\u[0-9a-fA-F]{4}')

def dsn_from_env() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL") or os.getenv("PGDATABASE_URL")
    if not dsn:
        print("[FATAL] No DATABASE_URL / DATABASE_PUBLIC_URL en el entorno", file=sys.stderr)
        sys.exit(2)
    sep = '&' if '?' in dsn else '?'
    if 'client_encoding=' not in dsn:
        dsn = f"{dsn}{sep}client_encoding=utf8"
    return dsn

def normalize_text(txt: Optional[str]) -> Tuple[Optional[str], bool]:
    if not txt:
        return txt, False
    if not UNICODE_ESCAPE_RE.search(txt):
        return txt, False
    try:
        fixed = json.loads(f'"{txt}"')
        if fixed != txt:
            return fixed, True
        return txt, False
    except Exception:
        try:
            fixed = bytes(txt, "utf-8").decode("unicode_escape")
            if fixed != txt:
                return fixed, True
            return txt, False
        except Exception:
            return txt, False

def fix_products_batch(conn, limit: int = 500) -> int:
    updated = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, name, description
            FROM products
            WHERE name ~ E'\\\\u[0-9a-fA-F]{4}' OR description ~ E'\\\\u[0-9a-fA-F]{4}'
            ORDER BY id
            LIMIT %s
            """,
            (limit,)
        )
        rows = cur.fetchall()

        for pid, name, desc in rows:
            new_name, ch_n = normalize_text(name)
            new_desc, ch_d = normalize_text(desc)
            if ch_n or ch_d:
                cur.execute(
                    "UPDATE products SET name = %s, description = %s WHERE id = %s",
                    (new_name, new_desc, pid)
                )
                updated += 1
    return updated

def main():
    dry_run = "--dry-run" in sys.argv
    limit = None
    for a in sys.argv:
        if a.startswith("--limit="):
            try:
                limit = int(a.split("=", 1)[1])
            except Exception:
                pass

    dsn = dsn_from_env()
    print(f"[INFO] Conectando…")
    with psycopg.connect(dsn, autocommit=False) as conn:
        try:
            conn.execute("SET client_encoding TO 'UTF8';")
        except Exception:
            pass

        total = 0
        while True:
            batch = fix_products_batch(conn, limit=limit or 500)
            if batch == 0:
                break
            total += batch
            if dry_run:
                conn.rollback()
                print(f"[DRY-RUN] batch {batch}, total {total}")
                break
            else:
                conn.commit()
                print(f"[OK] batch {batch}, total {total}")

    print(f"[DONE] Filas actualizadas: {total}")

if __name__ == "__main__":
    main()
PY