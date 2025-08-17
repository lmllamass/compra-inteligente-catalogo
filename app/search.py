# app/search.py
import httpx
from typing import Iterable

TIMEOUT = httpx.Timeout(connect=5.0, read=20.0, write=10.0, pool=5.0)  # ajusta si hace falta
HEADERS = {
    "User-Agent": "CompraInteligenteBot/1.0",
    "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

def fetch_xml_stream(url: str) -> Iterable[bytes]:
    with httpx.Client(timeout=TIMEOUT, headers=HEADERS) as client:
        # reintentos simples: 1 intento + 2 reintentos en errores de conexión/timeout
        for attempt in range(3):
            try:
                with client.stream("GET", url) as r:
                    r.raise_for_status()
                    for chunk in r.iter_bytes():
                        if chunk:
                            yield chunk
                    return
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout):
                if attempt == 2:
                    raise
                continue
