from fastapi import FastAPI, Query
import httpx
import xmltodict

DATERIUM_USERID = "0662759feb731be6fd95c59c4bad9f5209286336"
DATERIUM_URL = "https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php"

app = FastAPI(title="Buscador Ferretería – Directo Daterium")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/buscar")
def buscar(q: str = Query(..., min_length=3)):
    """
    Busca directamente en Daterium y devuelve lista de productos.
    """
    params = {
        "userID": DATERIUM_USERID,
        "searchbox": q
    }
    try:
        # Petición a Daterium
        resp = httpx.get(DATERIUM_URL, params=params, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        return {"error": f"Error al llamar a Daterium: {str(e)}"}

    try:
        data = xmltodict.parse(resp.text)
    except Exception as e:
        return {"error": f"Error al parsear XML: {str(e)}"}

    # Navegamos por la estructura esperada: resultados -> ficha
    fichas = []
    try:
        fichas = data["resultados"]["ficha"]
        if isinstance(fichas, dict):
            fichas = [fichas]  # un solo producto
    except KeyError:
        return {"productos": []}

    productos = []
    for f in fichas:
        # Extraer referencia principal
        referencia = None
        try:
            referencia = f.get("referencias", {}).get("referencia")
        except AttributeError:
            pass

        # Si referencia es lista, coge la primera
        if isinstance(referencia, list) and referencia:
            referencia = referencia[0]

        productos.append({
            "nombre": f.get("nombre", ""),
            "descripcion": f.get("descripcion", ""),
            "marca": f.get("marca", ""),
            "img": f.get("img280x240") or f.get("img500x500") or "",
            "ean": referencia.get("ean") if referencia else "",
            "pvp": float(referencia.get("pvp", 0)) if referencia else 0
        })

    return {"productos": productos}
