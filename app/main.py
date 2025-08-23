import os
import httpx
from lxml import etree
from urllib.parse import quote
from fastapi import FastAPI

app = FastAPI(
    title="Ferretero API",
    description="API para catalogo de herramientas",
    version="1.0.0"
)

DATERIUM_USER_ID = "0662759feb731be6fd95c59c4bad9f5209286336"

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
    """Test de parseo de productos de Daterium"""
    url = f"https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php?userID={quote(DATERIUM_USER_ID)}&searchbox={quote(marca)}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code != 200:
                return {"error": f"HTTP {response.status_code}", "url": url}
            
            # Parsear XML
            root = etree.fromstring(response.content)
            productos = []
            
            for ficha in root.xpath('.//ficha'):
                try:
                    producto = {
                        "id": ficha.findtext('id'),
                        "nombre": ficha.findtext('nombre'),
                        "marca": ficha.findtext('marca'),
                        "familia": ficha.findtext('familia'),
                        "subfamilia": ficha.findtext('subfamilia'),
                        "proveedor": ficha.findtext('proveedor'),
                        "relevancia": ficha.get('relevancia'),
                        "thumb": ficha.findtext('thumb'),
                        "img500": ficha.findtext('img500x500')
                    }
                    productos.append(producto)
                except Exception as e:
                    productos.append({"error": str(e)})
            
            return {
                "marca_buscada": marca,
                "total_encontrados": len(productos),
                "productos": productos[:5],  # Solo primeros 5 para test
                "xml_size": len(response.content)
            }
            
        except Exception as e:
            return {"error": str(e), "url": url}
