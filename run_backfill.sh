#!/usr/bin/env bash
set -euo pipefail

BASE="https://ferretero-api-production.up.railway.app"
TOKEN="KZ-setup-2025-08"

echo "Antes:"
curl -s "$BASE/admin/count_missing_ean?token=$TOKEN" | jq .

curl -s -X POST \
  "$BASE/admin/backfill_ean_loop?token=$TOKEN&batch=100&loops=20&pause_ms=150&dry=false" \
  -H "accept: application/json" -H "content-type: application/json" -d '{}' \
| jq '{ok, runs_count: (.runs|length), last: .runs[-1]|{updated, errors, scanned, tried, more}}'

echo "Después:"
curl -s "$BASE/admin/count_missing_ean?token=$TOKEN" | jq .
