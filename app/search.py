# app/search.py
from __future__ import annotations
import os
from typing import Any, Dict, List, Optional

import psycopg
from fastapi import APIRouter, HTTPException, Query, Path

router = APIRouter(tags=["productos"])

# ----------------- helpers -----------------
def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("PGDATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL no está definido")
    return dsn

def _conn():
    return psycopg.connect(_dsn())

def _wp_ficha_url(daterium_id: Optional[int], internal_id: int) -> str:
    # Preferimos el id de Daterium si existe (tu WP usa ese ID en la ruta)
    pid = str(daterium_id) if daterium_id else str(internal_id)
    return f"https://konkabeza.com/ferretero/producto/{pid}/"

def _google_url(nombre: str | None, ean: str | None) -> Optional[str]:
    q = (ean or "").strip() or (nombre or "").strip()
    if not q:
        return None
    return f"https://www.google.com/search?q={q.replace(' ', '+')}"

# ----------------- /buscar -----------------
@router.get("/buscar")
def buscar(
    q: str = Query(..., min_length=2, description="Texto libre: nombre, marca, ref, EAN"),
    marca: Optional[str] = Query(None, description="Filtro por marca (ILIKE)"),
    familia: Optional[str] = Query(None, description="Filtro por familia (ILIKE)"),
    subfamilia: Optional[str] = Query(None, description="Filtro por subfamilia (ILIKE)"),
    limit: int = Query(30, ge=1, le=100, description="Máximo de resultados"),
):
    """
    Busca productos en la base local (products + brands + families).
    Coincide por nombre/descripcion/EAN y permite filtros por marca/familia/subfamilia.
    """
    try:
        sql = """
        SELECT
            p.id AS pid,
            p.daterium_id,
            p.name,
            p.description,
            p.ean,
            p.pvp,
            p.thumb_url,
            p.image_url,
            b.name AS brand_name,
            b.logo_url AS brand_logo,
            f.name AS subfamily_name,
            pf.name AS family_name
        FROM products p
        LEFT JOIN brands   b  ON b.id = p.brand_id
        LEFT JOIN families f  ON f.id = p.family_id
        LEFT JOIN families pf ON pf.id = f.parent_id
        WHERE
            (
              p.name ILIKE %(q)s
              OR COALESCE(p.description,'') ILIKE %(q)s
              OR COALESCE(p.ean,'') ILIKE %(q_exact)s
            )
        """
        params = {
            "q": f"%{q}%",
            "q_exact": f"%{q}%",
        }
        if marca:
            sql += " AND b.name ILIKE %(marca)s"
            params["marca"] = f"%{marca}%"
        if familia:
            sql += " AND pf.name ILIKE %(familia)s"
            params["familia"] = f"%{familia}%"
        if subfamilia:
            sql += " AND f.name ILIKE %(subfamilia)s"
            params["subfamilia"] = f"%{subfamilia}%"

        sql += " ORDER BY b.name NULLS LAST, p.name LIMIT %(limit)s"
        params["limit"] = limit

        out: List[Dict[str, Any]] = []
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for (
                    pid,
                    daterium_id,
                    name,
                    description,
                    ean,
                    pvp,
                    thumb_url,
                    image_url,
                    brand_name,
                    brand_logo,
                    subfamily_name,
                    family_name,
                ) in cur.fetchall():
                    out.append({
                        "id": daterium_id or pid,        # devolvemos el ID "útil" para WP/GPT
                        "internal_id": pid,              # por si te hace falta
                        "daterium_id": daterium_id,
                        "nombre": name,
                        "descripcion": description,
                        "marca": brand_name,
                        "familia": family_name,
                        "subfamilia": subfamily_name,
                        "ean": ean,
                        "pvp": float(pvp) if pvp is not None else None,
                        "thumb": thumb_url,
                        "img": image_url or thumb_url,
                        "brand_logo": brand_logo,
                        "url_ficha": _wp_ficha_url(daterium_id, pid),
                    })

        return {"ok": True, "total": len(out), "productos": out}

    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Error en búsqueda: {ex}")

# ----------------- /ficha/{id} -----------------
@router.get("/ficha/{id}")
def ficha_by_path(id: str = Path(..., description="ID de Daterium o ID interno")):
    return _ficha_common(id)

@router.get("/ficha")
def ficha_by_query(id: str = Query(..., description="ID de Daterium o ID interno")):
    return _ficha_common(id)

def _ficha_common(id: str):
    """
    Devuelve la ficha de producto desde la base:
    - Si id es numérico y existe como daterium_id → prioriza esa coincidencia
    - Si no, busca por id interno (products.id)
    Incluye imágenes (product_images), logo de marca y enlaces útiles.
    """
    try:
        # preparar condiciones
        id_num = None
        try:
            id_num = int(id)
        except Exception:
            pass

        sql = """
        SELECT
            p.id AS pid,
            p.daterium_id,
            p.name,
            p.description,
            p.ean,
            p.pvp,
            p.thumb_url,
            p.image_url,
            b.name AS brand_name,
            b.logo_url AS brand_logo,
            f.name AS subfamily_name,
            pf.name AS family_name
        FROM products p
        LEFT JOIN brands   b  ON b.id = p.brand_id
        LEFT JOIN families f  ON f.id = p.family_id
        LEFT JOIN families pf ON pf.id = f.parent_id
        WHERE
            (%(idnum)s IS NOT NULL AND p.daterium_id = %(idnum)s)
            OR
            (%(idnum)s IS NOT NULL AND p.id = %(idnum)s)
        LIMIT 1
        """
        # Si id no es numérico, nunca encontrará nada. Forzamos 0 para no reventar
        params = {"idnum": id_num if id_num is not None else -1}

        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Producto no encontrado")

                (
                    pid,
                    daterium_id,
                    name,
                    description,
                    ean,
                    pvp,
                    thumb_url,
                    image_url,
                    brand_name,
                    brand_logo,
                    subfamily_name,
                    family_name,
                ) = row

                # imágenes
                cur.execute(
                    "SELECT url, is_primary FROM product_images WHERE product_id = %s ORDER BY is_primary DESC, id",
                    (pid,),
                )
                imgs = [{"url": u, "is_primary": bool(ip)} for (u, ip) in cur.fetchall()]

        ficha = {
            "id": daterium_id or pid,
            "internal_id": pid,
            "daterium_id": daterium_id,
            "nombre": name,
            "descripcion": description,
            "marca": brand_name,
            "brand_logo": brand_logo,
            "familia": family_name,
            "subfamilia": subfamily_name,
            "ean": ean,
            "pvp": float(pvp) if pvp is not None else None,
            "thumb": thumb_url,
            "img": image_url or (imgs[0]["url"] if imgs else None),
            "imagenes": imgs,
            "url_ficha_wp": _wp_ficha_url(daterium_id, pid),
            "google_url": _google_url(name, ean),
        }
        return ficha

    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Error en ficha: {ex}")