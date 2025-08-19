-- extensiones (si no existen)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- índices útiles
CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_products_brand ON products (brand_id);
CREATE INDEX IF NOT EXISTS idx_products_family ON products (family_id);
CREATE INDEX IF NOT EXISTS idx_products_ean ON products (ean);

-- por si consultas por texto en descripción
CREATE INDEX IF NOT EXISTS idx_products_desc_trgm ON products USING gin (description gin_trgm_ops);
