#!/usr/bin/env bash
set -u
BASE="https://ferretero-api-production.up.railway.app"
TOKEN="KZ-setup-2025-08"
SLEEP=20
NUM=10   # cuántos recientes mostrar

pretty() { if command -v python3 >/dev/null 2>&1; then python3 -m json.tool 2>/dev/null || cat; else cat; fi }

while true; do
  clear
  echo "=== Compra Inteligente – Monitor ETL ==="
  date; echo

  echo "-> /admin/count"
  curl -s "${BASE}/admin/count?token=${TOKEN}" | pretty
  echo

  echo "-> /admin/ingest_stats (si existe)"
  RESP=$(curl -s -w "\n%{http_code}" "${BASE}/admin/ingest_stats?token=${TOKEN}")
  BODY=$(printf "%s" "$RESP" | sed '$d'); CODE=$(printf "%s" "$RESP" | tail -n1)
  if [ "$CODE" = "200" ]; then echo "$BODY" | pretty; else echo "(endpoint no disponible)"; fi
  echo

  echo "-> /admin/recent?limit=${NUM}"
  curl -s "${BASE}/admin/recent?token=${TOKEN}&limit=${NUM}" | pretty

  echo; echo "Refresco en ${SLEEP}s… (Ctrl+C para salir)"
  sleep "$SLEEP"
done