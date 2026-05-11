#!/bin/bash
# SNF Model Builder — Postgres load script
# lens_id:            discogs_v1
# translator_version: 1.0.0
#
# Usage:
#   export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
#   bash load.sh
#
# Requires psql on PATH. Runs DDL then COPY for all spoke tables.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -z "$DATABASE_URL" ]; then
  echo "ERROR: DATABASE_URL not set."
  echo "  export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb"
  exit 1
fi

echo "Running DDL..."
psql "$DATABASE_URL" -f 00_ddl.sql

echo "Loading spoke tables..."
psql "$DATABASE_URL" -c "\COPY \"discogs\".\"snf_who\" (entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) FROM '$(pwd)/snf_who.csv' CSV HEADER"
psql "$DATABASE_URL" -c "\COPY \"discogs\".\"snf_what\" (entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) FROM '$(pwd)/snf_what.csv' CSV HEADER"
psql "$DATABASE_URL" -c "\COPY \"discogs\".\"snf_when\" (entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) FROM '$(pwd)/snf_when.csv' CSV HEADER"
psql "$DATABASE_URL" -c "\COPY \"discogs\".\"snf_how\" (entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) FROM '$(pwd)/snf_how.csv' CSV HEADER"
psql "$DATABASE_URL" -c "\COPY \"discogs\".\"snf_hub\" (entity_id, nucleus, label, sublabel, lens_id, translator_version) FROM '$(pwd)/snf_hub.csv' CSV HEADER"

echo "Done. Substrate loaded."
echo "Verify: psql \"$DATABASE_URL\" -c \"SELECT COUNT(*) FROM \"discogs\".snf_hub;\""
