-- Crear esquema e índices para búsqueda
CREATE SCHEMA IF NOT EXISTS catalogo_v2;

-- Extensiones útiles
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Tablas maestras
CREATE TABLE IF NOT EXISTS catalogo_v2.marcas (
  id TEXT PRIMARY KEY,
  nombre TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS catalogo_v2.familias (
  id TEXT PRIMARY KEY,
  nombre TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS catalogo_v2.subfamilias (
  id TEXT PRIMARY KEY,
  familia_id TEXT NOT NULL REFERENCES catalogo_v2.familias(id),
  nombre TEXT NOT NULL
);

-- Productos (índice intermedio)
CREATE TABLE IF NOT EXISTS catalogo_v2.productos (
  id_daterium TEXT PRIMARY KEY,
  nombre TEXT NOT NULL,
  marca_id TEXT REFERENCES catalogo_v2.marcas(id),
  familia_id TEXT REFERENCES catalogo_v2.familias(id),
  subfamilia_id TEXT REFERENCES catalogo_v2.subfamilias(id),
  descripcion TEXT
);

-- Índices para búsqueda
CREATE INDEX IF NOT EXISTS idx_productos_nombre_trgm
  ON catalogo_v2.productos USING gin (nombre gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_productos_desc_trgm
  ON catalogo_v2.productos USING gin (descripcion gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_productos_marca ON catalogo_v2.productos(marca_id);
CREATE INDEX IF NOT EXISTS idx_productos_familia ON catalogo_v2.productos(familia_id, subfamilia_id);
