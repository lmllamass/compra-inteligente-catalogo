# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import os
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.search import search_products, get_product_by_id

# -----------------------------------------------------------
# Configuración básica
# -----------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
logger = logging.getLogger("compra-inteligente")

# Orígenes CORS (coma-separados). Ej: "https://konkabeza.com,https://chat.openai.com"
_allowed = os.getenv("ALLOWED_ORIGINS", "https://konkabeza.com").strip()
ALLOWED_ORIGINS: List[str] = [o.strip() for o in _allowed.split(",") if o.strip()]

app = FastAPI(
    title="Compra Inteligente – Backend",
    version="1.0.0",
    description="API de búsqueda y fichas (con Daterium) para WordPress y GPT.",
)

# CORS (docs: FastAPI CORSMiddleware)
# https://fastapi.tiangolo.com/tutorial/cors/
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# -----------------------------------------------------------
# Healthcheck y raíz
# -----------------------------------------------------------
@app.get("/health", tags=["infra"])
def health():
    """
    Endpoint simple para healthcheck de Railway / balanceadores.
    """
    return {"ok": True}

@app.get("/", tags=["infra"])
def root():
    return {
        "name": "Compra Inteligente – Backend",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": ["/buscar", "/ficha", "/health"],
    }

# -----------------------------------------------------------
# BÚSQUEDA
# -----------------------------------------------------------
@app.get("/buscar", tags=["productos"])
def buscar(
    q: str = Query(..., min_length=2, description="Texto de búsqueda (nombre, marca, ref, EAN)"),
    limit: int = Query(30, ge=1, le=100, description="Límite de resultados (1-100)"),
):
    """
    Devuelve una lista normalizada de productos.

    Usa `search_products()` que:
    - Descarga XML de Daterium **en streaming** (httpx.iter_bytes)
    - Parsea incrementalmente con lxml (libera memoria)
    - Cachea (Redis si REDIS_URL; si no, TTL en memoria)
    """
    try:
        productos = search_products(q, limit=limit)
        # Si quieres, agrega aquí la url_ficha "canónica" para WP:
        for p in productos:
            pid = str(p.get("id") or "").strip()
            if pid:
                p["url_ficha"] = f"https://konkabeza.com/ferretero/producto/{pid}/"
        return {"productos": productos}
    except Exception as ex:
        logger.exception("Error en /buscar: %s", ex)
        raise HTTPException(status_code=502, detail="Error al consultar proveedor")

# -----------------------------------------------------------
# FICHA
# -----------------------------------------------------------
@app.get("/ficha", tags=["productos"])
def ficha(
    id: str = Query(..., description="ID de Daterium (o EAN, se intenta coincidir)"),
):
    """
    Devuelve la ficha de un producto por ID (o EAN).
    Estrategia:
      - Busca en Daterium por ese identificador
      - Si hay coincidencia exacta por `id` o contiene el `ean`, la devuelve
      - Si no, devuelve el primer resultado (fallback), o 404 si vacío
    """
    try:
        prod = get_product_by_id(id)
        if not prod:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        # Complementos útiles para la ficha en WP
        pid = str(prod.get("id") or "").strip()
        nombre = str(prod.get("nombre") or "").strip()
        ean = str(prod.get("ean") or "").strip()
        # Búsqueda Google (por EAN si hay, si no por nombre)
        query_google = ean if ean else nombre
        if query_google:
            prod["google_url"] = f"https://www.google.com/search?q={query_google.replace(' ', '+')}"
        prod["url_ficha_wp"] = f"https://konkabeza.com/ferretero/producto/{pid}/" if pid else None
        return prod
    except HTTPException:
        raise
    except Exception as ex:
        logger.exception("Error en /ficha: %s", ex)
        raise HTTPException(status_code=502, detail="Error al consultar proveedor")

# -----------------------------------------------------------
# NOTAS DE DESPLIEGUE (Railway / Uvicorn)
# -----------------------------------------------------------
# Start command recomendado (Railway):
#   uvicorn app.main:app --host 0.0.0.0 --port $PORT
#
# Docs:
# - CORS en FastAPI y añadir middlewares con add_middleware()  → https://fastapi.tiangolo.com/tutorial/cors/
# - Ejecutar con Uvicorn / despliegue manual                   → https://fastapi.tiangolo.com/deployment/manually/
# - Middlewares en FastAPI (referencia)                         → https://fastapi.tiangolo.com/reference/middleware/
#
# httpx streaming (usado en app/search.py):
# - Quickstart / stream, iter_bytes()                           → https://www.python-httpx.org/quickstart/
# - API .iter_bytes()                                           → https://www.python-httpx.org/api/
