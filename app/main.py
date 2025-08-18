# app/main.py
from __future__ import annotations
import os
from typing import List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Routers
from app import admin as admin_router
from app import search as search_router

# ----- CORS -----
_allowed = os.getenv("ALLOWED_ORIGINS", "https://konkabeza.com").strip()
ALLOWED_ORIGINS: List[str] = [o.strip() for o in _allowed.split(",") if o.strip()]

# ----- APP -----
app = FastAPI(
    title="Compra Inteligente – Backend",
    version="1.0.0",
    description="API de catálogo intermedio (Postgres) y utilidades admin."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ----- Routers -----
app.include_router(admin_router.router)       # /admin/*
app.include_router(search_router.router)      # /buscar, /ficha

# ----- Infra -----
@app.get("/health", tags=["infra"])
def health():
    return {"ok": True}

@app.get("/", tags=["infra"])
def root():
    return {
        "name": "Compra Inteligente – Backend",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": ["/buscar", "/ficha", "/admin/*", "/health"],
    }

# Start Command en Railway:
# uvicorn app.main:app --host 0.0.0.0 --port $PORT