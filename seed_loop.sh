#!/usr/bin/env bash
set -euo pipefail

API="https://ferretero-api-production.up.railway.app"
TOKEN="KZ-setup-2025-08"

# Lotes de queries (puedes añadir/editar libremente)
BATCHES=(
  "tornillo,tuerca,arandela,anclaje,taco%20quimico"
  "broca,metrica,roscadora,extractor%20de%20tornillos"
  "disco%20corte,disco%20desbaste,corona%20circular"
  "atornillado,puntas%20atornillar,portapuntas,vasos,llave%20fija,llave%20mixta,llave%20inglesa,alicate"
  "destornillador,martillo,mazo,formon,cincel"
  "adhesivo,epoxi,poliuretano,silicona,sellador"
  "fijatornillos,aflojatodo,desengrasante,limpiador%20industrial"
  "brida,terminal,clema,conector%20rapido,canaleta"
  "guante%20nitrilo,mascara%20soldadura,gafa%20seguridad,casco%20obra,proteccion%20auditiva"
  "manguito,rapido%20neumatico,teflon,abrazadera,grifo%20bola"
  "andamio,borriqueta,puntal,regla%20obra,cutter"
)

SLEEP_BETWEEN_CALLS=2         # segundos entre llamadas dentro del lote
SLEEP_BETWEEN_BATCHES=20      # segundos entre lotes
MAX_RETRIES=3                 # reintentos por llamada
STATE_FILE=".seed_loop.state" # para reanudar desde donde se quedó

run_seed () {
  local q="$1"
  local attempt=1
  while true; do
    echo ">> $(date '+%F %T') Seed: $q (intento $attempt)"
    RESP=$(curl -s -X POST "$API/admin/seed_basic?token=$TOKEN&queries=$q")
    echo "$RESP" | jq . 2>/dev/null || echo "$RESP"
    # éxito si ok:true
    echo "$RESP" | grep -q '"ok": true' && return 0

    if (( attempt >= MAX_RETRIES )); then
      echo "!! Falló tras $MAX_RETRIES intentos: $q"
      return 1
    fi
    attempt=$((attempt+1))
    sleep 5
  done
}

progress_snapshot () {
  echo "-- PROGRESS --"
  curl -s "$API/admin/count?token=$TOKEN" | jq . || curl -s "$API/admin/count?token=$TOKEN"
  echo
}

# Cargar último índice procesado (si existe)
START_IDX=0
if [[ -f "$STATE_FILE" ]]; then
  START_IDX=$(cat "$STATE_FILE" | tr -d '\n' || echo 0)
fi

# Bucle principal
for ((i=START_IDX; i<${#BATCHES[@]}; i++)); do
  Q="${BATCHES[$i]}"

  # Lanza el lote completo (una sola llamada seed_basic por lote para evitar “exceso”)
  if run_seed "$Q"; then
    echo "$((i+1))" > "$STATE_FILE"   # actualiza progreso
    progress_snapshot
  else
    echo "!! Lote con errores, sigo con el siguiente…"
  fi

  # espera entre lotes
  sleep "$SLEEP_BETWEEN_BATCHES"
done

echo "== COMPLETADO =="
progress_snapshot
