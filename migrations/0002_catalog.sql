-- ============================================
-- Catálogo intermedio (marcas, familias, productos)
-- ============================================

-- Extensiones útiles (idempotentes)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==================
-- Tabla: brands
-- ==================
CREATE TABLE IF NOT EXISTS brands (
  id            BIGSERIAL PRIMARY KEY,
  name          TEXT NOT NULL UNIQUE,
  daterium_id   BIGINT,
  logo_url      TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==================
-- Tabla: families
-- (puede modelar familia / subfamilia vía parent_id)
-- ==================
CREATE TABLE IF NOT EXISTS families (
  id            BIGSERIAL PRIMARY KEY,
  name          TEXT NOT NULL UNIQUE,
  parent_id     BIGINT REFERENCES families(id) ON DELETE SET NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==================
-- Tabla: products
-- ==================
CREATE TABLE IF NOT EXISTS products (
  id             BIGSERIAL PRIMARY KEY,
  daterium_id    BIGINT UNIQUE,             -- id del proveedor (Daterium)
  name           TEXT NOT NULL,
  description    TEXT,
  brand_id       BIGINT REFERENCES brands(id) ON DELETE SET NULL,
  family_id      BIGINT REFERENCES families(id) ON DELETE SET NULL,
  ean            TEXT,
  sku            TEXT,
  pvp            NUMERIC(12,2),
  thumb_url      TEXT,
  image_url      TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==================
-- Tabla: product_images (n imágenes por producto)
-- ==================
CREATE TABLE IF NOT EXISTS product_images (
  id           BIGSERIAL PRIMARY KEY,
  product_id   BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  url          TEXT NOT NULL,
  is_primary   BOOLEAN NOT NULL DEFAULT FALSE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ==================
-- Tabla: product_synonyms (búsqueda mejorada)
-- ==================
CREATE TABLE IF NOT EXISTS product_synonyms (
  id           BIGSERIAL PRIMARY KEY,
  product_id   BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  term         TEXT NOT NULL
);

-- ==================
-- Tabla: ingest_log (auditoría de cargas)
-- ==================
CREATE TABLE IF NOT EXISTS ingest_log (
  id           BIGSERIAL PRIMARY KEY,
  source       TEXT NOT NULL,         -- p.ej. 'daterium'
  items        INTEGER NOT NULL,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at  TIMESTAMPTZ
);

-- ======== Índices útiles ========
CREATE INDEX IF NOT EXISTS idx_products_brand   ON products(brand_id);
CREATE INDEX IF NOT EXISTS idx_products_family  ON products(family_id);
CREATE INDEX IF NOT EXISTS idx_products_name    ON products USING GIN (to_tsvector('spanish', name));
CREATE INDEX IF NOT EXISTS idx_products_desc    ON products USING GIN (to_tsvector('spanish', coalesce(description,'')));
CREATE INDEX IF NOT EXISTS idx_products_ean     ON products(ean);
CREATE TABLE IF NOT EXISTS ingest_cursor (
  id          BIGSERIAL PRIMARY KEY,
  strategy    TEXT NOT NULL,          -- 'ngrams' | 'brands' | 'families' | 'digits'
  cursor_key  TEXT NOT NULL,          -- última clave procesada (por ejemplo 'aa')
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_cursor_strategy ON ingest_cursor(strategy);