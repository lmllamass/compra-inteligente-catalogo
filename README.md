# Compra Inteligente – Catálogo Intermedio

API + ETL para indexar marcas/familias/subfamilias/productos de Daterium y exponer búsqueda por texto. 

## 1) Variables de entorno

- `DATABASE_URL` → cadena Postgres
- `DB_SCHEMA` → por defecto `catalogo_v2`
- `DATERIUM_USERID` → tu userID de Daterium
- `DATERIUM_BASE_URL` → `https://api.dateriumsystem.com`

Crea `.env` (opcional en local):

```ini
DATABASE_URL=postgresql://user:pass@host:5432/dbname
DB_SCHEMA=catalogo_v2
DATERIUM_USERID=tu_user_id
```

## 2) Migración inicial

Ejecuta `migrations/0001_init.sql` en la base de datos (Railway Query o psql).

## 3) Ejecutar ETL

- Todo: `python -m etl.load_all`
- Solo referencias: `python -m etl.load_refs`
- Solo productos: `python -m etl.load_products`

## 4) API (FastAPI)

- `GET /health` → `{ status: ok }`
- `GET /buscar?q=atornillador&limit=25` → resultados con `id_daterium` para luego pedir la ficha completa a Daterium.

**Comando de arranque**:

```
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## 5) Despliegue en Railway (con GitHub)

1. Sube este repo a GitHub.
2. En Railway → New → **GitHub Repo** → selecciona el repo.
3. Variables en Railway:
   - `DATABASE_URL`
   - `DB_SCHEMA=catalogo_v2`
   - `DATERIUM_USERID=...`
   - `DATERIUM_BASE_URL=https://api.dateriumsystem.com`
4. Service (Web): Command → `uvicorn app.main:app --host 0.0.0.0 --port 8080`
5. Crea un **One-off Job** para ETL: `python -m etl.load_all`

## 6) Notas sobre Daterium

- Los endpoints y el formato XML pueden variar. En `etl/load_refs.py` y `etl/load_products.py` hay comentarios `*** Ajusta estos paths ...` para mapear el XML real.
- Si Daterium requiere otros parámetros de POST (filtros o paginación), añádelos en `fetch_products_page`.

## 7) Búsqueda

La búsqueda usa índices `pg_trgm` en `nombre` y `descripcion`. Ajusta el `ORDER BY similarity(...)` o agrega filtros por `marca_id`, `familia_id`, etc.
