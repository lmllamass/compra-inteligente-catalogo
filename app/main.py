"""
API Ferretero - Versión Mínima Funcional
"""
from fastapi import FastAPI

# Crear app sin configuraciones avanzadas
app = FastAPI(
    title="Ferretero API",
    description="API para catálogo de herramientas",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {
        "message": "Ferretero API funcionando",
        "status": "ok",
        "version": "1.0.0"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/ping")
async def ping():
    return {"ping": "pong"}
