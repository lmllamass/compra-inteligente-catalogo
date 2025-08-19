# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Routers de la propia app
# Importa DESPUÉS de crear app si tuvieras dependencias circulares
from app import admin as admin_router
from app.search import router as search_router  # asumiendo que ya existe

app = FastAPI(title="Ferretero API", version="1.0.0")

# CORS permisivo para pruebas (ajusta después)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

# Orden: primero admin, luego público
app.include_router(admin_router.router)
app.include_router(search_router)