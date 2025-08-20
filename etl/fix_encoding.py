# etl/fix_encoding.py
from __future__ import annotations
import os
import re
import sys
import psycopg
from html import unescape

# ------------------------------
# Config / helpers
# ------------------------------

def dsn() -> str:
    for k in ("DATABASE_URL", "DATABASE_PUBLIC_URL", "PGDATABASE_URL"):
        v = os.getenv(k)
        if v:
            return v
    raise RuntimeError("No DATABASE_URL/DATABASE_PUBLIC_URL/PGDATABASE_URL in env")

# \uXXXX -> carácter real (p. ej. "est\u00e1ndar" -> "estándar")
_u_pat = re.compile(r"\\u([0-9a-fA-F]{4})")

def _unescape_u_sequences(s: str) -> str:
    def _sub(m: re.Match[str]) -> str:
        cp = int(m.group(1), 16)
        return chr(cp)
    return _u_pat.sub(_sub, s)

# Mojibake típico UTF-8 leído como latin1 (no exhaustivo pero práctico)
MOJIBAKE_MAP = {
    "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã³": "ó", "Ãº": "ú",
    "ÃÁ": "Á", "Ã‰": "É", "Ã\x8d": "Í", "Ã“": "Ó", "Ãš": "Ú",
    "Ã±": "ñ", "Ã‘": "Ñ",
    "Ã¼": "ü", "Ãœ": "Ü",
    "Â¿": "¿", "Â¡": "¡",
    "Âº": "º", "Âª": "ª",
    "â": "–", "â": "—",
    "â": "“", "â": "”", "â": "’",
    "â¢": "•", "â¦": "…",
    "Â·": "·",
    "â": "Δ",
    "â": "∙",
    "â¤": "≤",
    "â¥": "≥",
    "â": "√",
    "â": "∏",
    "â": "∐",
    "â": "∙",
    "â": "−",
    "â": "∗",
    # añade aquí si ves más patrones
}

_mojibake_re = re.compile("|".join(map(re.escape, MOJIBAKE_MAP.keys()))) if MOJIBAKE_MAP else None

def _fix_mojibake(s: str) -> str:
    if not s:
        return s
    if _mojibake_re:
        s = _mojibake_re.sub(lambda m: MOJIBAKE_MAP[m.group(0)], s)
    return s

def normalize_text(s: str | None) -> tuple[str, bool]:
    """
    Devuelve (texto_normalizado, cambió?)
    """
    if s is None:
        return "", False

    original = s

    # 1) HTML entities → texto
    s = unescape(s)

    # 2) \uXXXX → carácter real
    s = _unescape_u_sequences(s)

    # 3) Mojibake común
    s = _fix_mojibake(s)

    # 4) Limpieza menor
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = s.strip()

    return s, (s != original)

# ------------------------------
# Lógica por lotes
# ------------------------------

# patrón “\uXXXX” literal en la base (dos barras invertidas en texto)
PATTERN_BACKSLASH_U = r"\\u[0-9A-Fa-f]{4}"

def fix_products_batch(conn, limit: int = 500) -> int:
    r"""
    Corrige un lote de filas en products que presenten \uXXXX o mojibake en name o description.
    Devuelve cuántas filas se actualizaron.
    """
    updated = 0
    sql = """
        SELECT id, name, description
        FROM products
        WHERE name ~ %s OR description ~ %s
           OR name ILIKE %s OR description ILIKE %s
        ORDER BY id
        LIMIT %s
    """
    like_mojibake = "%Ã%"  # heurística: muchos mojibake empiezan por “Ã”
    with conn.cursor() as cur:
        cur.execute(sql, (PATTERN_BACKSLASH_U, PATTERN_BACKSLASH_U, like_mojibake, like_mojibake, limit))
        rows = cur.fetchall()

    if not rows:
        return 0

    with conn.cursor() as cur:
        for pid, name, desc in rows:
            new_name, ch_n = normalize_text(name or "")
            new_desc, ch_d = normalize_text(desc or "")
            if ch_n or ch_d:
                cur.execute(
                    "UPDATE products SET name = %s, description = %s WHERE id = %s",
                    (new_name, new_desc, pid),
                )
                updated += 1

    return updated

# ------------------------------
# CLI
# ------------------------------

def main():
    dry_run = "--dry-run" in sys.argv
    limit = 500
    for a in sys.argv:
        if a.startswith("--limit="):
            try:
                limit = int(a.split("=", 1)[1])
            except ValueError:
                pass

    with psycopg.connect(dsn(), autocommit=not dry_run) as conn:
        # Si es dry-run, no autocommit y hacemos rollback al final.
        if dry_run:
            with conn.transaction():
                n = fix_products_batch(conn, limit=limit)
                print(f"[DRY-RUN] Rows to update: {n}")
                conn.rollback()
        else:
            with conn.transaction():
                n = fix_products_batch(conn, limit=limit)
                print(f"[FIX] Updated rows: {n}")
            # commit implícito por transaction context

if __name__ == "__main__":
    main()