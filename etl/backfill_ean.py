# etl/backfill_ean.py
from __future__ import annotations

import argparse
import os
import re
import time
from typing import Optional, List

import httpx
import psycopg
from lxml import etree
from urllib.parse import quote


# ---------------- DSN helper (igual lógica que admin.py) ----------------
def dsn() -> str:
    """
    Prioridad: DATABASE_URL > PGDATABASE_URL > DATABASE_PUBLIC_URL
    - Si host es interno (railway.internal / IPv6 fd..) => sslmode=disable
    - Si host es público => sslmode=require
    """
    import re as _re
    for key in ("DATABASE_URL", "PGDATABASE_URL", "DATABASE_PUBLIC_URL"):
        val = os.getenv(key)
        if not val:
            continue
        sep = "&" if "?" in val else "?"
        host = None
        try:
            host = _re.search(r'@([^/:]+)', val).group(1)
        except Exception:
            pass
        is_private = False
        if host:
            if ".railway.internal" in host:
                is_private = True
            if ":" in host and host.lower().startswith("fd"):
                is_private = True
        if "sslmode=" not in val:
            val = f"{val}{sep}{'sslmode=disable' if is_private else 'sslmode=require'}"
        return val
    raise RuntimeError("No DATABASE_URL/PGDATABASE_URL/DATABASE_PUBLIC_URL set")


# ---------------- HTTP y XML ----------------
def http_client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(connect=6.0, read=40.0, write=10.0, pool=10.0),
        headers={"User-Agent": "CompraInteligente/1.0", "Accept": "application/xml"},
    )


def fetch_xml_by_id(user_id: str, daterium_id: int) -> Optional[bytes]:
    """
    Daterium no expone un endpoint 'by id' público; usamos la búsqueda con el id como searchbox.
    """
    url = (
        "https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php"
        f"?userID={quote(user_id)}&searchbox={quote(str(daterium_id))}"
    )
    with http_client() as c:
        r = c.get(url)
        if r.status_code != 200 or not r.content:
            return None
        return r.content


EAN_RE = re.compile(r"^\s*(\d[\d\s\-]{6,20})\s*$")  # captura candidatos numéricos con separadores


def normalize_ean(txt: str) -> Optional[str]:
    """
    Limpia separadores y valida longitud plausible (8, 12, 13, 14).
    Prefiere EAN-13 si se le pide seleccionar entre varios (lo haremos fuera).
    """
    if not txt:
        return None
    m = EAN_RE.match(txt)
    if not m:
        return None
    digits = re.sub(r"[\s\-]", "", m.group(1))
    if len(digits) in (8, 12, 13, 14):
        return digits
    return None


def extract_all_eans(xml_bytes: bytes) -> List[str]:
    """
    Extrae TODOS los EAN que aparezcan en cualquier <referencias>/<referencia>/ean.
    Si no hay, intenta buscar etiquetas 'ean' en cualquier parte.
    """
    out: List[str] = []
    root = etree.fromstring(xml_bytes)

    # 1) Ruta estándar
    for e in root.xpath(".//referencias/referencia/ean"):
        val = normalize_ean((e.text or "").strip())
        if val and val not in out:
            out.append(val)

    # 2) Fallback muy laxo (cualquier <ean>)
    if not out:
        for e in root.xpath(".//ean"):
            val = normalize_ean((e.text or "").strip())
            if val and val not in out:
                out.append(val)

    return out


def prefer_ean(eans: List[str]) -> Optional[str]:
    """
    Si hay varios EAN válidos, prioriza EAN-13, luego 14, luego 12, luego 8.
    """
    if not eans:
        return None
    # orden de preferencia
    pref = [13, 14, 12, 8]
    for L in pref:
        candidates = [x for x in eans if len(x) == L]
        if candidates:
            return candidates[0]
    return eans[0]


# ---------------- DB helpers ----------------
def get_batch_without_ean(conn, limit: int) -> List[tuple]:
    """
    Devuelve lista de (id, daterium_id) para productos sin EAN.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, daterium_id FROM products WHERE ean IS NULL ORDER BY id ASC LIMIT %s",
            (limit,),
        )
        return cur.fetchall()


def update_ean(conn, prod_id: int, ean: str) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE products SET ean=%s WHERE id=%s", (ean, prod_id))


# ---------------- Main job ----------------
def main():
    ap = argparse.ArgumentParser(description="Backfill de EAN desde Daterium")
    ap.add_argument("--limit", type=int, default=1000, help="Tamaño del lote (por tanda)")
    ap.add_argument("--sleep", type=float, default=0.15, help="Pausa entre llamadas a Daterium (seg)")
    ap.add_argument("--dry-run", action="store_true", help="No escribe en BD")
    ap.add_argument("--max-batches", type=int, default=0, help="0=sin límite; si >0, número de tandas y salir")
    args = ap.parse_args()

    user_id = os.getenv("DATERIUM_USER_ID", "").strip()
    if not user_id:
        raise SystemExit("Falta DATERIUM_USER_ID en el entorno")

    dsn_str = dsn()
    updated_total = 0
    batches_done = 0

    # Conexión con pequeños reintentos
    last_err = None
    for _ in range(3):
        try:
            conn = psycopg.connect(
                dsn_str,
                connect_timeout=8,
                options="-c statement_timeout=60000 -c idle_in_transaction_session_timeout=60000",
                autocommit=False,
            )
            break
        except Exception as e:
            last_err = e
            time.sleep(1.0)
    else:
        raise SystemExit(f"No se pudo conectar a la BD: {last_err}")

    try:
        while True:
            batch = get_batch_without_ean(conn, args.limit)
            if not batch:
                print(f"[OK] No hay más productos sin EAN. Total actualizados: {updated_total}")
                break

            updated_this_batch = 0

            for prod_id, daterium_id in batch:
                if daterium_id is None:
                    continue  # sin referencia para consultar

                # Pide ficha a Daterium y parsea EANs
                try:
                    xml_bytes = fetch_xml_by_id(user_id, int(daterium_id))
                except Exception:
                    xml_bytes = None

                if not xml_bytes:
                    time.sleep(args.sleep)
                    continue

                eans = extract_all_eans(xml_bytes)
                ean_final = prefer_ean(eans)

                if ean_final and not args.dry_run:
                    try:
                        update_ean(conn, prod_id, ean_final)
                        updated_this_batch += 1
                    except Exception:
                        # si falla un update, no paramos; seguirá con el resto
                        pass

                time.sleep(args.sleep)

            if args.dry_run:
                print(f"[DRY-RUN] Lote: {len(batch)} | EANs candidatos (no escritos): {updated_this_batch}")
                # rollback explícito en dry-run
                conn.rollback()
            else:
                conn.commit()
                updated_total += updated_this_batch
                print(f"[OK] Lote: {len(batch)} | EANs actualizados: {updated_this_batch} | Total: {updated_total}")

            batches_done += 1
            if args.max-batches and batches_done >= args.max_batches:  # type: ignore[attr-defined]
                print(f"[STOP] Alcanzado max-batches={args.max_batches}. Total actualizados: {updated_total}")
                break

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()