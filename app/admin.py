from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import APIKeyQuery
from sqlalchemy import text
import os

from app.db import get_db

router = APIRouter(prefix="/admin", tags=["admin"])

# Lee el token desde variables de entorno
MIGRATION_TOKEN = os.getenv("MIGRATION_TOKEN")
api_key_query = APIKeyQuery(name="token", auto_error=False)

def check_token(token: str = Depends(api_key_query)):
    if not token or token != MIGRATION_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return token

@router.post("/migrate")
def run_migrations(db=Depends(get_db), token: str = Depends(check_token)):
    """Ejecuta las migraciones SQL"""
    try:
        with open("migrations/0002_catalog.sql", "r") as f:
            sql = f.read()
        db.execute(text(sql))
        db.commit()
        return {"status": "ok", "message": "Migración ejecutada correctamente"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# Debug para verificar que el token se carga bien
@router.get("/debug_token_status")
def debug_token_status(token: str = Depends(api_key_query)):
    return {
        "provided": token,
        "expected": MIGRATION_TOKEN,
        "ok": token == MIGRATION_TOKEN,
    }