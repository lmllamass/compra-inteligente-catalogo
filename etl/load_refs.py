import httpx
import xmltodict
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db import get_session
from app.models import marcas, familias, subfamilias
from app.settings import settings

CATALOGO_ENDPOINT = f"{settings.DATERIUM_BASE_URL}/catalogo_fc_xml.php"

async def fetch_catalogo(client: httpx.AsyncClient):
    resp = await client.post(CATALOGO_ENDPOINT, data={"userID": settings.DATERIUM_USERID}, timeout=settings.HTTP_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return xmltodict.parse(resp.text)

async def load_refs():
    assert settings.DATERIUM_USERID, "DATERIUM_USERID no configurado"
    async with httpx.AsyncClient() as client:
        data = await fetch_catalogo(client)

    # *** Ajusta estos paths a la estructura real del XML ***
    marcas_list = []
    familias_list = []
    subfamilias_list = []

    try:
        for m in data['catalogo']['marcas']['marca']:
            marcas_list.append({"id": str(m['id']), "nombre": m['nombre']})
    except Exception:
        pass

    try:
        for f in data['catalogo']['familias']['familia']:
            familias_list.append({"id": str(f['id']), "nombre": f['nombre']})
    except Exception:
        pass

    try:
        for s in data['catalogo']['subfamilias']['subfamilia']:
            subfamilias_list.append({
                "id": str(s['id']),
                "familia_id": str(s['familia_id']),
                "nombre": s['nombre']
            })
    except Exception:
        pass

    with get_session() as session:
        if marcas_list:
            stmt = pg_insert(marcas).values(marcas_list)
            stmt = stmt.on_conflict_do_update(index_elements=[marcas.c.id], set_={"nombre": stmt.excluded.nombre})
            session.execute(stmt)
        if familias_list:
            stmt = pg_insert(familias).values(familias_list)
            stmt = stmt.on_conflict_do_update(index_elements=[familias.c.id], set_={"nombre": stmt.excluded.nombre})
            session.execute(stmt)
        if subfamilias_list:
            stmt = pg_insert(subfamilias).values(subfamilias_list)
            stmt = stmt.on_conflict_do_update(index_elements=[subfamilias.c.id], set_={
                "familia_id": stmt.excluded.familia_id,
                "nombre": stmt.excluded.nombre,
            })
            session.execute(stmt)
        session.commit()

if __name__ == "__main__":
    import asyncio
    asyncio.run(load_refs())
