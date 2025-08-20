# etl/backfill_ean.py
from __future__ import annotations
import os, sys, time, re
import psycopg, httpx
from lxml import etree
from urllib.parse import quote

def dsn() -> str:
    for k in ("DATABASE_URL", "DATABASE_PRIVATE_URL", "PGDATABASE_URL", "DATABASE_PUBLIC_URL"):
        v = os.getenv(k)
        if v: return v
    raise RuntimeError("No DB DSN")

_EAN_CAND_RE = re.compile(r"\b(\d{8}|\d{12,14})\b")

def _gtin_checksum_ok(code: str) -> bool:
    n = len(code)
    if n not in (8,12,13,14) or not code.isdigit(): return False
    s=0
    for i,c in enumerate(reversed(code[:-1]), start=1):
        s += int(c) * (3 if i%2==1 else 1)
    return ((10 - (s % 10)) % 10) == int(code[-1])

def _extract_eans_from_ficha(ficha) -> list[str]:
    eans=[]
    for ref in ficha.xpath(".//referencias/referencia"):
        for tag in ("ean","ean13","gtin","codigo_barras"):
            val=(ref.findtext(tag) or "").strip()
            if val and val.isdigit(): eans.append(val)
        for tag in ("sku","codigo","ref","referencia"):
            val=(ref.findtext(tag) or "").strip()
            if val: eans.extend(_EAN_CAND_RE.findall(val))
    for tag in ("nombre","descripcion","descripcioncorta"):
        txt=(ficha.findtext(tag) or "").strip()
        if txt: eans.extend(_EAN_CAND_RE.findall(txt))
    uniq=[]
    for e in eans:
        e=e.strip()
        if e not in uniq and _gtin_checksum_ok(e):
            uniq.append(e)
    uniq.sort(key=lambda x: (len(x)!=13, len(x)))
    return uniq

def _http():
    return httpx.Client(
        timeout=httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=5.0),
        headers={"User-Agent": "CompraInteligente/1.0", "Accept": "application/xml"},
    )

def fetch_ficha(user_id: str, did: int):
    url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(user_id)}&searchbox={quote(str(did))}"
    with _http() as c:
        r = c.get(url)
        if r.status_code != 200:
            return None
    root = etree.fromstring(r.content)
    return root.find(".//ficha")

def main():
    user_id = os.getenv("DATERIUM_USER_ID","").strip()
    if not user_id:
        print("[FATAL] Falta DATERIUM_USER_ID", file=sys.stderr); sys.exit(1)

    limit = 500
    sleep_s = 0.2
    for a in sys.argv[1:]:
        if a.startswith("--limit="):
            limit = int(a.split("=",1)[1])
        elif a.startswith("--sleep="):
            sleep_s = float(a.split("=",1)[1])

    updated = 0
    with psycopg.connect(dsn(), autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, daterium_id FROM products
                WHERE ean IS NULL AND daterium_id IS NOT NULL
                ORDER BY id ASC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        if not rows:
            print("[OK] No hay productos pendientes (ean NULL).")
            return

        for pid, did in rows:
            time.sleep(sleep_s)
            ficha = fetch_ficha(user_id, did)
            if not ficha: 
                continue
            eans = _extract_eans_from_ficha(ficha)
            if not eans: 
                continue
            ean = eans[0]
            with conn.cursor() as cur:
                cur.execute("UPDATE products SET ean = %s WHERE id = %s", (ean, pid))
                updated += 1
        conn.commit()
    print(f"[OK] Actualizados EAN: {updated} (limit={limit})")

if __name__ == "__main__":
    main()