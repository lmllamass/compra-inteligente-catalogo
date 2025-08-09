from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
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

# --- Diagnóstico sin panel de Postgres ---
@app.get("/count_productos")
def count_productos(session = Depends(get_session)):
    total = session.execute(text("SELECT COUNT(*) FROM catalogo_v2.productos")).scalar()
    return {"total_productos": int(total or 0)}

@app.get("/count_refs")
def count_refs(session = Depends(get_session)):
    q = """
    SELECT
      (SELECT COUNT(*) FROM catalogo_v2.marcas)      AS marcas,
      (SELECT COUNT(*) FROM catalogo_v2.familias)    AS familias,
      (SELECT COUNT(*) FROM catalogo_v2.subfamilias) AS subfamilias
    """
    row = session.execute(text(q)).mappings().one()
    return {k:int(v or 0) for k,v in row.items()}

@app.get("/db_ping")
def db_ping(session = Depends(get_session)):
    one = session.execute(text("SELECT 1")).scalar()
    return {"db": "ok" if one == 1 else "fail"}
