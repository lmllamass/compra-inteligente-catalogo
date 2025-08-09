import httpx
import xmltodict
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db import get_session
from app.models import productos
from app.settings import settings

BUSQUEDA_ENDPOINT = f"{settings.DATERIUM_BASE_URL}/busqueda_avanzada_fc_xml.php"

async def fetch_products_page(client: httpx.AsyncClient, page: int = 1, page_size: int = 200):
    payload = {
        "userID": settings.DATERIUM_USERID,
        "page": page,
        "per_page": page_size,
    }
    resp = await client.post(BUSQUEDA_ENDPOINT, data=payload, timeout=settings.HTTP_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return xmltodict.parse(resp.text)

async def load_products(max_pages: int = 100):
    assert settings.DATERIUM_USERID, "DATERIUM_USERID no configurado"

    collected = 0
    async with httpx.AsyncClient() as client:
        for page in range(1, max_pages + 1):
            data = await fetch_products_page(client, page=page)

            items = []
            try:
                productos_xml = data['resultado']['productos']['producto']
                if isinstance(productos_xml, dict):
                    productos_xml = [productos_xml]
            except Exception:
                productos_xml = []

            for pr in productos_xml:
                try:
                    items.append({
                        "id_daterium": str(pr.get("id")),
                        "nombre": pr.get("nombre") or pr.get("descripcion_corta") or "(sin nombre)",
                        "marca_id": str(pr.get("marca_id")) if pr.get("marca_id") else None,
                        "familia_id": str(pr.get("familia_id")) if pr.get("familia_id") else None,
                        "subfamilia_id": str(pr.get("subfamilia_id")) if pr.get("subfamilia_id") else None,
                        "descripcion": pr.get("descripcion")
                    })
                except Exception:
                    continue

            if not items:
                break

            with get_session() as session:
                stmt = pg_insert(productos).values(items)
                stmt = stmt.on_conflict_do_update(index_elements=[productos.c.id_daterium], set_={
                    "nombre": stmt.excluded.nombre,
                    "marca_id": stmt.excluded.marca_id,
                    "familia_id": stmt.excluded.familia_id,
                    "subfamilia_id": stmt.excluded.subfamilia_id,
                    "descripcion": stmt.excluded.descripcion,
                })
                session.execute(stmt)
                session.commit()

            collected += len(items)
            if len(items) < 1:
                break

    print(f"Productos cargados/actualizados: {collected}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(load_products())
