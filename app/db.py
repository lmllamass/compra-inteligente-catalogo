from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .settings import settings

assert settings.DATABASE_URL, "DATABASE_URL no definida"

engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# Dependencia de FastAPI para obtener sesión por request
from contextlib import contextmanager

@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
