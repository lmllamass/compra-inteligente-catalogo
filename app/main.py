from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from .db import get_session
from .search import buscar_productos

app = FastAPI(title="Compra Inteligente – Catálogo Intermedio")

@app.get("/health")
def health():
    return {"status": "ok"}

class SearchResponse(BaseModel):
    id_daterium: str
    nombre: str
    marca_id: str | None = None
    familia_id: str | None = None
    subfamilia_id: str | None = None
    descripcion: str | None = None

@app.get("/buscar", response_model=list[SearchResponse])
def buscar(q: str, limit: int = 25, session = Depends(get_session)):
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="Parámetro q demasiado corto")
    return buscar_productos(session, q, limit)
