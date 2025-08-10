from fastapi import FastAPI, Query
from typing import Any, Dict, List
import httpx, xmltodict

DATERIUM_USERID = "0662759feb731be6fd95c59c4bad9f5209286336"
DATERIUM_URL = "https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php"

app = FastAPI(title="Buscador Ferretería – Directo Daterium")

@app.get("/health")
def health():
    return {"status": "ok"}

def as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}

def first_or_none(x: Any):
    if isinstance(x, list): return x[0] if x else None
    return x

@app.get("/buscar")
def buscar(q: str = Query(..., min_length=3)):
    # llamada directa
    try:
        r = httpx.get(DATERIUM_URL, params={"userID": DATERIUM_USERID, "searchbox": q}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        return {"error": f"HTTP error: {e}"}

    # parseo XML robusto
    try:
        data = xmltodict.parse(r.text)
    except Exception as e:
        return {"error": f"XML parse error: {e}"}

    # rutas posibles: buscador>resultados>ficha  |  resultados>ficha  |  resultados>fichas>ficha
    root = data.get("buscador") or data
    resultados = as_dict(root.get("resultados"))
    fichas = resultados.get("ficha") or as_dict(resultados.get("fichas")).get("ficha") or []
    if isinstance(fichas, dict):
        fichas = [fichas]
    if not isinstance(fichas, list):
        return {"productos": []}

    productos: List[Dict[str, Any]] = []
    for f in fichas:
        f = as_dict(f)

        # ID del producto (texto del nodo <id>)
        prod_id = f.get("id")
        if isinstance(prod_id, dict):
            # cuando viene como {'@cont': '0', '#text': '1571488'}
            prod_id = prod_id.get("#text") or prod_id.get("@value") or ""

        # referencias (si existen)
        referencias = f.get("referencias")
        ref = None
        if isinstance(referencias, dict):
            ref = referencias.get("referencia")
        elif isinstance(referencias, list):
            ref = referencias[0]
        ref = first_or_none(ref)
        ref = as_dict(ref)

        # imagen: prioriza thumb, luego tamaños conocidos
        img = f.get("thumb") or f.get("img280x240") or f.get("img500x500") or f.get("img") or ""

        # marca puede venir como texto o dict con atributos
        marca = f.get("marca")
        if isinstance(marca, dict):
            marca = marca.get("#text") or ""

        # pvp y ean desde la referencia si existe
        try:
            pvp = float(ref.get("pvp", 0)) if ref else 0.0
        except Exception:
            pvp = 0.0
        ean = ref.get("ean") if ref else ""

        productos.append({
            "id": (prod_id or "").strip(),
            "nombre": (f.get("nombre") or "").strip(),
            "descripcion": (f.get("descripcion") or "").strip(),
            "marca": (marca or "").strip(),
            "img": img or "",
            "ean": ean or "",
            "pvp": pvp,
        })

    return {"productos": productos}

# Debug opcional
@app.get("/debug_raw")
def debug_raw(q: str = Query(..., min_length=3), key: str = ""):
    if key != "ok":
        return {"error": "unauthorized"}
    r = httpx.get(DATERIUM_URL, params={"userID": DATERIUM_USERID, "searchbox": q}, timeout=30)
    return {"status_code": r.status_code, "len": len(r.text), "sample": r.text[:1200]}
