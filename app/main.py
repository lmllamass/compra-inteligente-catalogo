# app/main.py
from __future__ import annotations

import os
import logging
from typing import List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.search import search_products, get_product_by_id
from app import admin as admin_router  # router admin (migración y ver tablas)

# ------------------ Logging ------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
logger = logging.getLogger("compra-inteligente")

# ------------------ CORS ------------------
_allowed = os.getenv("ALLOWED_ORIGINS", "https://konkabeza.com").strip()
ALLOWED_ORIGINS: List[str] = [o.strip() for o in _allowed.split(",") if o.strip()]

# ------------------ FastAPI app ------------------
app = FastAPI(
    title="Compra Inteligente – Backend",
    version="1.0.0",
    description="API de búsqueda/fichas + utilidades de administración.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Importante: incluir routers DESPUÉS de crear la app
app.include_router(admin_router.router)

# ------------------ Infra ------------------
@app.get("/health", tags=["infra"])
def health():
    return {"ok": True}

@app.get("/", tags=["infra"])
def root():
    return {
        "name": "Compra Inteligente – Backend",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": ["/buscar", "/ficha", "/health"],
    }

# ------------------ Productos ------------------
@app.get("/buscar", tags=["productos"])
def buscar(
    q: str = Query(..., min_length=2, description="Texto: nombre, marca, ref, EAN"),
    limit: int = Query(30, ge=1, le=100, description="Límite de resultados"),
):
    try:
        productos = search_products(q, limit=limit)
        # URL canónica a la ficha en WP
        for p in productos:
            pid = str(p.get("id") or "").strip()
            if pid:
                p["url_ficha"] = f"https://konkabeza.com/ferretero/producto/{pid}/"
        return {"productos": productos}
    except Exception as ex:
        logger.exception("Error en /buscar: %s", ex)
        raise HTTPException(status_code=502, detail="Error al consultar proveedor")

@app.get("/ficha", tags=["productos"])
def ficha(id: str = Query(..., description="ID Daterium o EAN")):
    try:
        prod = get_product_by_id(id)
        if not prod:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        pid = str(prod.get("id") or "").strip()
        nombre = str(prod.get("nombre") or "").strip()
        ean = str(prod.get("ean") or "").strip()

        # Búsqueda Google (por EAN si hay, si no por nombre)
        query_google = ean if ean else nombre
        if query_google:
            prod["google_url"] = f"https://www.google.com/search?q={query_google.replace(' ', '+')}"

        if pid:
            prod["url_ficha_wp"] = f"https://konkabeza.com/ferretero/producto/{pid}/"

        return prod
    except HTTPException:
        raise
    except Exception as ex:
        logger.exception("Error en /ficha: %s", ex)
        raise HTTPException(status_code=502, detail="Error al consultar proveedor")

# ------------------ Uvicorn (Railway) ------------------
# Start Command en Railway:
# uvicorn app.main:app --host 0.0.0.0 --port $PORT