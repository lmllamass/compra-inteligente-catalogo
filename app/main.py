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

def matches_all_terms(text: str, terms: list[str]) -> bool:
    t = (text or "").lower()
    return all(term in t for term in terms)

@app.get("/buscar")
def buscar(
    q: str = Query(..., min_length=3, description="Texto de búsqueda"),
    limit: int = Query(12, ge=1, le=30, description="Máximo de resultados")
):
    # 1) Llamada a Daterium
    try:
        r = httpx.get(DATERIUM_URL, params={"userID": DATERIUM_USERID, "searchbox": q}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        return {"error": f"HTTP error: {e}"}

    # 2) Parseo XML
    try:
        data = xmltodict.parse(r.text)
    except Exception as e:
        return {"error": f"XML parse error: {e}"}

    # 3) Extraer fichas reales
    root = data.get("buscador") or data
    resultados = as_dict(root.get("resultados"))
    fichas = resultados.get("ficha") or as_dict(resultados.get("fichas")).get("ficha") or []
    if isinstance(fichas, dict):
        fichas = [fichas]
    if not isinstance(fichas, list):
        return {"productos": []}

    # 4) Convertir y filtrar por términos (para recortar respuesta)
    terms = [t.strip().lower() for t in q.split() if t.strip()]
    productos: List[Dict[str, Any]] = []

    for f in fichas:
        f = as_dict(f)

        prod_id = f.get("id")
        if isinstance(prod_id, dict):
            prod_id = prod_id.get("#text") or prod_id.get("@value") or ""

        referencias = f.get("referencias")
        ref = None
        if isinstance(referencias, dict):
            ref = referencias.get("referencia")
        elif isinstance(referencias, list):
            ref = referencias[0]
        ref = first_or_none(ref)
        ref = as_dict(ref)

        img = f.get("thumb") or f.get("img280x240") or f.get("img500x500") or f.get("img") or ""
        marca = f.get("marca")
        if isinstance(marca, dict):
            marca = marca.get("#text") or ""

        try:
            pvp = float(ref.get("pvp", 0)) if ref else 0.0
        except Exception:
            pvp = 0.0
        ean = ref.get("ean") if ref else ""

        nombre = (f.get("nombre") or "").strip()
        descripcion = (f.get("descripcion") or "").strip()

        # Filtrado por términos para acotar (nombre+descripcion+marca)
        blob = " ".join([nombre, descripcion, marca or ""])
        if terms and not matches_all_terms(blob, terms):
            continue

        productos.append({
            "id": (prod_id or "").strip(),
            "nombre": nombre,
            "descripcion": descripcion,
            "marca": (marca or "").strip(),
            "img": img or "",
            "ean": ean or "",
            "pvp": pvp,
        })

        if len(productos) >= limit:  # corte duro
            break

    return {"productos": productos}
