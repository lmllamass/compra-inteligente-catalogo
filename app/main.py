from fastapi import FastAPI, Query
from typing import Any, Dict, List
import httpx, xmltodict

DATERIUM_USERID = "0662759feb731be6fd95c59c4bad9f5209286336"
DATERIUM_URL = "https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php"

app = FastAPI(title="Buscador Ferretería – Directo Daterium")

@app.get("/health")
def health():
    return {"status": "ok"}

def to_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}

def first_or_none(x: Any):
    if isinstance(x, list): return x[0] if x else None
    return x

@app.get("/buscar")
def buscar(q: str = Query(..., min_length=3)):
    try:
        resp = httpx.get(DATERIUM_URL, params={"userID": DATERIUM_USERID, "searchbox": q}, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        return {"error": f"HTTP error: {e}"}

    try:
        data = xmltodict.parse(resp.text)
    except Exception as e:
        return {"error": f"XML parse error: {e}", "raw_len": len(resp.text)}

    # Soporta posibles variantes: resultados/fichas/ficha o resultados/ficha
    resultados = data.get("resultados") or data.get("resultado") or {}
    fichas = resultados.get("ficha") or resultados.get("fichas") or []
    if isinstance(fichas, dict):
        fichas = [fichas]
    if not isinstance(fichas, list):
        return {"productos": []}

    productos: List[Dict[str, Any]] = []
    for f in fichas:
        f = to_dict(f)

        # referencias puede ser dict, list o string
        referencias = f.get("referencias")
        referencia = None
        if isinstance(referencias, dict):
            referencia = referencias.get("referencia")
        elif isinstance(referencias, list):
            referencia = referencias[0]
        referencia = first_or_none(referencia)
        ref = to_dict(referencia)

        # campos imagen con fallback
        img = f.get("img280x240") or f.get("img500x500") or f.get("img") or ""

        # pvp y ean robustos
        try:
            pvp = float(ref.get("pvp", 0)) if ref else 0.0
        except Exception:
            pvp = 0.0
        ean = ref.get("ean") if ref else ""

        productos.append({
            "nombre": f.get("nombre", "") or "",
            "descripcion": f.get("descripcion", "") or "",
            "marca": f.get("marca", "") or "",
            "img": img or "",
            "ean": ean or "",
            "pvp": pvp,
        })

    return {"productos": productos}

# Endpoint opcional para ver la respuesta cruda (útil para debug puntual)
@app.get("/debug_raw")
def debug_raw(q: str = Query(..., min_length=3), key: str = ""):
    if key != "ok":  # protección simple
        return {"error": "unauthorized"}
    r = httpx.get(DATERIUM_URL, params={"userID": DATERIUM_USERID, "searchbox": q}, timeout=30)
    return {"status_code": r.status_code, "len": len(r.text), "sample": r.text[:1000]}
