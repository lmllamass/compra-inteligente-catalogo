#!/usr/bin/env bash
set -e

API="https://ferretero-api-production.up.railway.app"
TOKEN="KZ-setup-2025-08"

run() {
  local q="$1"
  echo ">> Seed: $q"
  curl -s -X POST "$API/admin/seed_basic?token=$TOKEN&queries=$q" | jq .
  sleep 2
}

# A – fijación, corte, perforación
run "tornillo,tuerca,arandela,anclaje,taco%20quimico"
run "broca,metrica,roscadora,extractor%20de%20tornillos"
run "disco%20corte,disco%20desbaste,corona%20circular"

# B – atornillado, mano
run "atornillado,puntas%20atornillar,portapuntas,vasos,llave%20fija,llave%20mixta,llave%20inglesa,alicate"
run "destornillador,martillo,mazo,formon,cincel"

# C – adhesivos y química
run "adhesivo,epoxi,poliuretano,silicona,sellador"
run "fijatornillos,aflojatodo,desengrasante,limpiador%20industrial"

# D – electricidad y epi
run "brida,terminal,clema,conector%20rapido,canaleta"
run "guante%20nitrilo,mascara%20soldadura,gafa%20seguridad,casco%20obra,proteccion%20auditiva"

# E – hidráulica/obra
run "manguito,rapido%20neumatico,teflon,abrazadera,grifo%20bola"
run "andamio,borriqueta,puntal,regla%20obra,cutter"

echo "== RESUMEN =="
curl -s "$API/admin/count?token=$TOKEN" | jq .
