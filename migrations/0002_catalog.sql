# 1) crear carpeta y migración
mkdir -p migrations

cat > migrations/0002_catalog.sql <<'SQL'
-- Migración 0002: catálogo intermedio (marcas, familias, productos, imágenes, sinónimos, log)
-- Usa pg_trgm para búsquedas rápidas por texto (trigramas).
-- Doc oficial: https://www.postgresql.org/docs/current/pgtrgm.html

BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- MARCAS
CREATE TABLE IF NOT EXISTS brands (
  brand_id   BIGSERIAL PRIMARY KEY,
  name       TEXT UNIQUE NOT NULL
);

-- FAMILIAS
CREATE TABLE IF NOT EXISTS families (
  family_id  BIGSERIAL PRIMARY KEY,
  name       TEXT UNIQUE NOT NULL
);

-- PRODUCTOS (product_id = id Daterium u otros proveedores)
CREATE TABLE IF NOT EXISTS products (
  product_id   TEXT PRIMARY KEY,
  brand_id     BIGINT REFERENCES brands(brand_id),
  family_id    BIGINT REFERENCES families(family_id),
  name         TEXT NOT NULL,
  description  TEXT,
  ean          TEXT,
  unit         TEXT,
  price        NUMERIC,
  source       TEXT DEFAULT 'daterium',
  updated_at   TIMESTAMPTZ DEFAULT now()
);

-- IMÁGENES (varias por producto)
CREATE TABLE IF NOT EXISTS product_images (
  product_id TEXT REFERENCES products(product_id) ON DELETE CASCADE,
  url        TEXT NOT NULL,
  priority   INT  DEFAULT 1,
  PRIMARY KEY (product_id, url)
);

-- SINÓNIMOS / TÉRMINOS (mejora recall de búsqueda)
CREATE TABLE IF NOT EXISTS product_synonyms (
  product_id TEXT REFERENCES products(product_id) ON DELETE CASCADE,
  term       TEXT NOT NULL,
  kind       TEXT,
  PRIMARY KEY (product_id, term)
);

-- LOG DE INGESTA
CREATE TABLE IF NOT EXISTS ingest_log (
  at         TIMESTAMPTZ DEFAULT now(),
  provider   TEXT NOT NULL,
  rows       INT  NOT NULL,
  ok         BOOLEAN NOT NULL,
  message    TEXT
);

-- ÍNDICES para búsqueda rápida (trigram y ean)
CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_products_ean       ON products (ean);
CREATE INDEX IF NOT EXISTS idx_brands_name_trgm   ON brands  USING GIN (name gin_trgm_ops);

COMMIT;
SQL

# 2) commit + push
git add migrations/0002_catalog.sql
git commit -m "feat(db): esquema catálogo (pg_trgm + índices) – migración 0002"
git push