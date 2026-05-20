import httpx
from fastapi import FastAPI
from .settings import settings
from .admin import router as admin_router

app = FastAPI(
    title="Ferretero API",
    description="API para catalogo de herramientas",
    version="1.0.0"
)

app.include_router(admin_router)


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


@app.get("/test_daterium/{marca}")
async def test_daterium(marca: str):
    """Test de conexión a Daterium (sin parsing XML)"""
    try:
        url = f"{settings.DATERIUM_BASE_URL}/busqueda_avanzada_fc_xml.php?userID={settings.DATERIUM_USERID}&searchbox={marca}"

        async with httpx.AsyncClient(timeout=settings.HTTP_TIMEOUT_SECONDS) as client:
            response = await client.get(url)

            return {
                "marca_buscada": marca,
                "url_llamada": url,
                "status_code": response.status_code,
                "content_type": response.headers.get("content-type"),
                "content_length": len(response.content),
                "xml_preview": response.text[:500] + "..." if len(response.text) > 500 else response.text
            }

    except Exception as e:
        return {"error": str(e), "type": type(e).__name__}
