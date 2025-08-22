-- migrations/0003_daterium_real.sql
-- ============================================
-- Migración para estructura real de Daterium
-- ============================================

-- Agregar campos específicos de Daterium a products
ALTER TABLE products 
ADD COLUMN IF NOT EXISTS supplier_name TEXT,
ADD COLUMN IF NOT EXISTS supplier_cif TEXT,
ADD COLUMN IF NOT EXISTS relevancia NUMERIC(5,2),
ADD COLUMN IF NOT EXISTS idcatalogo TEXT;

-- Quitar constraint NOT NULL de ean si existe (Daterium no siempre tiene EAN)
-- ALTER TABLE products ALTER COLUMN ean DROP NOT NULL;

-- ==================
-- Tabla: product_references (para múltiples SKUs/códigos)
-- ==================
CREATE TABLE IF NOT EXISTS product_references (
  id             BIGSERIAL PRIMARY KEY,
  product_id     BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  reference_code TEXT NOT NULL,
  reference_type TEXT,                    -- 'sku', 'model', 'internal', etc.
  supplier_name  TEXT,                    -- proveedor específico
  is_primary     BOOLEAN DEFAULT FALSE,  -- referencia principal
  created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_product_refs_product ON product_references(product_id);
CREATE INDEX IF NOT EXISTS idx_product_refs_code ON product_references(reference_code);
CREATE UNIQUE INDEX IF NOT EXISTS ux_product_refs_product_code 
  ON product_references(product_id, reference_code);

-- ==================
-- Tabla: aecoc_categories (categorías AECOC)
-- ==================
CREATE TABLE IF NOT EXISTS aecoc_categories (
  id           BIGSERIAL PRIMARY KEY,
  aecoc_id     TEXT NOT NULL UNIQUE,
  name         TEXT NOT NULL,
  parent_id    BIGINT REFERENCES aecoc_categories(id),
  level        INTEGER DEFAULT 0,
  created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aecoc_parent ON aecoc_categories(parent_id);
CREATE INDEX IF NOT EXISTS idx_aecoc_level ON aecoc_categories(level);

-- ==================
-- Tabla: product_aecoc (relación producto-categoría AECOC)
-- ==================
CREATE TABLE IF NOT EXISTS product_aecoc (
  id             BIGSERIAL PRIMARY KEY,
  product_id     BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  aecoc_id       BIGINT NOT NULL REFERENCES aecoc_categories(id),
  created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_product_aecoc 
  ON product_aecoc(product_id, aecoc_id);

-- ==================
-- Índices para búsqueda mejorada
-- ==================
CREATE INDEX IF NOT EXISTS idx_products_supplier ON products(supplier_name);
CREATE INDEX IF NOT EXISTS idx_products_relevancia ON products(relevancia DESC);
CREATE INDEX IF NOT EXISTS idx_products_idcatalogo ON products(idcatalogo);

-- Índices de texto completo para español
CREATE INDEX IF NOT EXISTS idx_products_fulltext_es 
  ON products USING GIN (to_tsvector('spanish', coalesce(name,'') || ' ' || coalesce(description,'')));

-- ==================
-- Vistas útiles
-- ==================
CREATE OR REPLACE VIEW v_products_complete AS
SELECT 
  p.id,
  p.daterium_id,
  p.name,
  p.description,
  p.pvp,
  p.thumb_url,
  p.image_url,
  p.supplier_name,
  p.supplier_cif,
  p.relevancia,
  p.idcatalogo,
  b.name AS brand_name,
  b.logo_url AS brand_logo,
  f.name AS subfamily_name,
  pf.name AS family_name,
  -- Concatenar categorías AECOC
  STRING_AGG(DISTINCT ac.name, ' > ' ORDER BY ac.level) AS aecoc_path
FROM products p
LEFT JOIN brands b ON b.id = p.brand_id
LEFT JOIN families f ON f.id = p.family_id
LEFT JOIN families pf ON pf.id = f.parent_id
LEFT JOIN product_aecoc pa ON pa.product_id = p.id
LEFT JOIN aecoc_categories ac ON ac.id = pa.aecoc_id
GROUP BY p.id, b.name, b.logo_url, f.name, pf.name;

-- ==================
-- Funciones útiles
-- ==================
CREATE OR REPLACE FUNCTION search_products_advanced(
  search_term TEXT,
  brand_filter TEXT DEFAULT NULL,
  family_filter TEXT DEFAULT NULL,
  supplier_filter TEXT DEFAULT NULL,
  limit_results INTEGER DEFAULT 50
)
RETURNS TABLE (
  id BIGINT,
  daterium_id BIGINT,
  name TEXT,
  description TEXT,
  brand_name TEXT,
  family_name TEXT,
  supplier_name TEXT,
  relevancia NUMERIC,
  thumb_url TEXT,
  image_url TEXT,
  similarity_score REAL
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    p.id,
    p.daterium_id,
    p.name,
    p.description,
    b.name AS brand_name,
    COALESCE(pf.name, f.name) AS family_name,
    p.supplier_name,
    p.relevancia,
    p.thumb_url,
    p.image_url,
    -- Calcular score de similitud
    GREATEST(
      similarity(p.name, search_term),
      similarity(COALESCE(p.description, ''), search_term),
      similarity(COALESCE(b.name, ''), search_term)
    ) AS similarity_score
  FROM products p
  LEFT JOIN brands b ON b.id = p.brand_id
  LEFT JOIN families f ON f.id = p.family_id
  LEFT JOIN families pf ON pf.id = f.parent_id
  WHERE 
    (
      p.name ILIKE '%' || search_term || '%'
      OR COALESCE(p.description, '') ILIKE '%' || search_term || '%'
      OR b.name ILIKE '%' || search_term || '%'
      OR to_tsvector('spanish', COALESCE(p.name,'') || ' ' || COALESCE(p.description,'')) 
         @@ plainto_tsquery('spanish', search_term)
    )
    AND (brand_filter IS NULL OR b.name ILIKE '%' || brand_filter || '%')
    AND (family_filter IS NULL OR pf.name ILIKE '%' || family_filter || '%' OR f.name ILIKE '%' || family_filter || '%')
    AND (supplier_filter IS NULL OR p.supplier_name ILIKE '%' || supplier_filter || '%')
  ORDER BY 
    p.relevancia DESC NULLS LAST,
    similarity_score DESC,
    p.name
  LIMIT limit_results;
END;
$ LANGUAGE plpgsql;