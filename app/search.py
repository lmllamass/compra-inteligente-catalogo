# app/search.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import quote

import httpx
from lxml import etree

# ========= Config =========
DATERIUM_USER_ID = os.getenv("DATERIUM_USER_ID", "").strip()
DATERIUM_BASE = "https://api.dateriumsystem.com/busqueda_avanzada_fc_xml.php"
HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=25.0, write=10.0, pool=5.0)
HTTP_HEADERS = {
    "User-Agent": "CompraInteligente/1.0 (+konkabeza.com)",
    "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

# ========= Caché (Redis si existe; si no, memoria local con TTL) =========
_redis = None
REDIS_URL = os.getenv("REDIS_URL", "").strip()
if REDIS_URL:
    try:
        import redis  # type: ignore
        _redis = redis.from_url(REDIS_URL)
    except Exception:
        _redis = None  # fallback a memoria

class _LocalTTLCache:
    def __init__(self, max_items: int = 512):
        self._data: Dict[str, tuple[float, object]] = {}
        self._max = max_items

    def get(self, key: str):
        now = time.time()
        item = self._data.get(key)
        if not item:
            return None
        exp, val = item
        if exp < now:
            self._data.pop(key, None)
            return None
        return val

    def set(self, key: str, value, ttl: int):
        # Evita crecer sin límite
        if len(self._data) >= self._max:
            # estrategia simple: borrar elementos expirados o el más antiguo
            now = time.time()
            expired = [k for k, (e, _) in self._data.items() if e < now]
            if expired:
                for k in expired:
                    self._data.pop(k, None)
            elif self._data:
                self._data.pop(next(iter(self._data)))
        self._data[key] = (time.time() + ttl, value)

_local_cache = _LocalTTLCache()

def cache_get(key: str):
    if _redis:
        try:
            raw = _redis.get(key)
            if raw:
                import json
                return json.loads(raw)
        except Exception:
            pass
    return _LocalTTLCache.get(_local_cache, key)

def cache_set(key: str, value, ttl: int = 900):
    if _redis:
        try:
            import json
            _redis.setex(key, ttl, json.dumps(value))
            return
        except Exception:
            pass
    _local_cache.set(key, value, ttl)

# ========= HTTP: descarga en streaming con reintentos =========
def _daterium_url(query: str) -> str:
    if not DATERIUM_USER_ID:
        raise RuntimeError("Falta DATERIUM_USER_ID en variables de entorno.")
    return f"{DATERIUM_BASE}?userID={quote(DATERIUM_USER_ID)}&searchbox={quote(query)}"

def fetch_xml_stream(url: str) -> Iterable[bytes]:
    """
    Descarga en streaming. Reintenta 2 veces en errores transitorios.
    """
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=HTTP_HEADERS) as client:
        for attempt in range(3):
            try:
                with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    for chunk in resp.iter_bytes():
                        if chunk:
                            yield chunk
                    return
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout):
                if attempt == 2:
                    raise
                # reintento
                continue

# ========= XML streaming parse =========
def _text(elem: etree._Element, tag: str) -> str:
    x = elem.findtext(tag)
    return (x or "").strip()

def _first_text(elem: Optional[etree._Element], tag: str) -> str:
    if elem is None:
        return ""
    x = elem.findtext(tag)
    return (x or "").strip()

def _best_image(ficha: etree._Element) -> str:
    # Priorizamos tamaños más grandes si existen
    candidates = [
        "img1000x1000", "img800x800", "img600x600",
        "img500x500", "img280x240", "img200x200",
        "thumb", "img"
    ]
    for t in candidates:
        val = _text(ficha, t)
        if val:
            return val
    return ""

def _extract_product(ficha: etree._Element) -> Dict[str, object]:
    # Algunas respuestas traen múltiples referencias; tomamos la primera
    ref = ficha.find(".//referencias/referencia")
    return {
        "id": _text(ficha, "id"),
        "nombre": _text(ficha, "nombre"),
        "marca": _text(ficha, "marca"),
        "descripcion": _text(ficha, "descripcion") or _text(ficha, "descripcioncorta"),
        "ean": _first_text(ref, "ean"),
        "precio": _safe_float(_first_text(ref, "pvp")),
        "unidades": _first_text(ref, "unidades") or _first_text(ref, "unidad"),
        "imagen": _best_image(ficha),
    }

def _safe_float(v: str) -> Optional[float]:
    try:
        v = v.replace(",", ".")
        f = float(v)
        return f
    except Exception:
        return None

def parse_products_from_xml_stream(chunks: Iterable[bytes], limit: int = 50) -> Iterator[Dict[str, object]]:
    """
    Parser incremental: procesa <ficha> a medida que llegan datos y libera memoria.
    """
    parser = etree.XMLPullParser(events=("end",))
    count = 0
    for chunk in chunks:
        parser.feed(chunk)
        for _, elem in parser.read_events():
            if elem.tag == "ficha":
                yield _extract_product(elem)
                elem.clear()  # libera memoria del nodo
                count += 1
                if count >= limit:
                    return

# ========= API interna para FastAPI =========
def search_products(query: str, limit: int = 30, cache_ttl: int = 900) -> List[Dict[str, object]]:
    q = (query or "").strip()
    if len(q) < 2:
        return []
    key = f"buscar:{q}:{limit}"
    cached = cache_get(key)
    if cached is not None:
        return cached  # type: ignore

    url = _daterium_url(q)
    chunks = fetch_xml_stream(url)
    items = list(parse_products_from_xml_stream(chunks, limit=max(1, min(limit, 100))))
    # Normaliza campos mínimos
    for p in items:
        p.setdefault("marca", "")
        p.setdefault("ean", "")
        p.setdefault("precio", None)
        p.setdefault("unidades", "")
        p.setdefault("imagen", "")
    cache_set(key, items, ttl=cache_ttl)
    return items

def get_product_by_id(product_id: str, cache_ttl: int = 900) -> Optional[Dict[str, object]]:
    pid = (product_id or "").strip()
    if not pid:
        return None
    key = f"ficha:{pid}"
    cached = cache_get(key)
    if cached is not None:
        return cached  # type: ignore

    # Estrategia simple: buscar por id y elegir la coincidencia exacta; si no, el primer resultado.
    url = _daterium_url(pid)
    chunks = fetch_xml_stream(url)
    best: Optional[Dict[str, object]] = None
    for p in parse_products_from_xml_stream(chunks, limit=50):
        if p.get("id") == pid or (p.get("ean") and pid in str(p.get("ean"))):
            best = p
            break
        if best is None:
            best = p
    if best:
        cache_set(key, best, ttl=cache_ttl)
    return best
