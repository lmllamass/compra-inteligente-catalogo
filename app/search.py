from sqlalchemy import text
from sqlalchemy.orm import Session
from .settings import settings

# Búsqueda simple por trigram en nombre y descripción
# Requiere la migración 0001 para pg_trgm y los índices

def buscar_productos(session: Session, q: str, limit: int = 25):
    schema = settings.DB_SCHEMA
    query = text(
        f"""
        SELECT p.id_daterium, p.nombre, p.marca_id, p.familia_id, p.subfamilia_id, p.descripcion
        FROM {schema}.productos p
        WHERE (
            p.nombre ILIKE :like
            OR (p.descripcion IS NOT NULL AND p.descripcion ILIKE :like)
        )
        ORDER BY similarity(p.nombre, :q) DESC
        LIMIT :limit
        """
    )
    like = f"%{q}%"
    rows = session.execute(query, {"q": q, "like": like, "limit": limit}).mappings().all()
    return [dict(r) for r in rows]
