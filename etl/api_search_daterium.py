# app/search_v2.py
from __future__ import annotations
import os
from typing import Any, Dict, List, Optional

import psycopg
from fastapi import APIRouter, HTTPException, Query, Path

router = APIRouter(tags=["productos"])

# ----------------- helpers -----------------
def _dsn() -> str:
    """Conexión robusta con múltiples fallbacks"""
    import re
    
    # Prioridad: DATABASE_URL > PGDATABASE_URL > DATABASE_PUBLIC_URL
    for key in ("DATABASE_URL", "PGDATABASE_URL", "DATABASE_PUBLIC_URL"):
        val = os.getenv(key)
        if not val:
            continue
            
        # Agregar sslmode según el host
        if "?" not in val:
            val += "?"
        else:
            val += "&"
            
        # Para Railway, usar require por defecto
        if "sslmode=" not in val:
            val += "sslmode=require"
            
        # Timeouts agresivos
        val += "&connect_timeout=10&statement_timeout=30000"
        
        return val
    
    raise RuntimeError("No DATABASE_URL disponible")

def _conn():
    """Conexión con reintentos"""
    import time
    dsn = _dsn()
    for attempt in range(3):
        try:
            return psycopg.connect(dsn)
        except Exception as e:
            if attempt == 2:
                raise HTTPException(
                    status_code=503, 
                    detail=f"DB no disponible después de 3 intentos: {e}"
                )
            time.sleep(2 ** attempt)  # backoff exponencial

def _wp_ficha_url(daterium_id: Optional[int], internal_id: int) -> str:
    # Preferimos el id de Daterium si existe
    pid = str(daterium_id) if daterium_id else str(internal_id)
    return f"https://konkabeza.com/ferretero/producto/{pid}/"

def _google_url(nombre: str | None, supplier: str | None) -> Optional[str]:
    """URL de búsqueda en Google combinando producto y marca/proveedor"""
    terms = []
    if nombre:
        terms.append(nombre.strip())
    if supplier:
        terms.append(supplier.strip())
    
    if not terms:
        return None
    
    query = " ".join(terms)
    return f"https://www.google.com/search?q={query.replace(' ', '+')}"

# ----------------- /buscar -----------------
@router.get("/buscar")
def buscar(
    q: str = Query(..., min_length=2, description="Texto libre: nombre, marca, proveedor"),
    marca: Optional[str] = Query(None, description="Filtro por marca (ILIKE)"),
    familia: Optional[str] = Query(None, description="Filtro por familia (ILIKE)"),
    proveedor: Optional[str] = Query(None, description="Filtro por proveedor (ILIKE)"),
    min_relevancia: Optional[float] = Query(None, description="Relevancia mínima (0-100)"),
    limit: int = Query(30, ge=1, le=100, description="Máximo de resultados"),
):
    """
    Busca productos usando la estructura real de Daterium.
    Incluye relevancia, proveedor, categorías AECOC y búsqueda por similitud.
    """
    try:
        # Usar la función de búsqueda avanzada de PostgreSQL
        sql = """
        SELECT * FROM search_products_advanced(
            %(search_term)s,
            %(brand_filter)s,
            %(family_filter)s,
            %(supplier_filter)s,
            %(limit_results)s
        )
        """
        
        params = {
            "search_term": q,
            "brand_filter": marca,
            "family_filter": familia,
            "supplier_filter": proveedor,
            "limit_results": limit,
        }
        
        # Si no existe la función, usar consulta manual
        fallback_sql = """
        SELECT 
            p.id AS id,
            p.daterium_id,
            p.name,
            p.description,
            b.name AS brand_name,
            COALESCE(pf.name, f.name) AS family_name,
            p.supplier_name,
            p.relevancia,
            p.thumb_url,
            p.image_url,
            -- Score de similitud simple
            CASE 
                WHEN p.name ILIKE %(q_exact)s THEN 1.0
                WHEN p.name ILIKE %(q)s THEN 0.8
                WHEN COALESCE(p.description,'') ILIKE %(q)s THEN 0.6
                WHEN b.name ILIKE %(q)s THEN 0.7
                ELSE 0.3
            END AS similarity_score
        FROM products p
        LEFT JOIN brands b ON b.id = p.brand_id
        LEFT JOIN families f ON f.id = p.family_id
        LEFT JOIN families pf ON pf.id = f.parent_id
        WHERE
            (
              p.name ILIKE %(q)s
              OR COALESCE(p.description,'') ILIKE %(q)s
              OR b.name ILIKE %(q)s
              OR p.supplier_name ILIKE %(q)s
            )
        """
        
        # Agregar filtros opcionales
        if marca:
            fallback_sql += " AND b.name ILIKE %(marca)s"
            params["marca"] = f"%{marca}%"
        if familia:
            fallback_sql += " AND (pf.name ILIKE %(familia)s OR f.name ILIKE %(familia)s)"
            params["familia"] = f"%{familia}%"
        if proveedor:
            fallback_sql += " AND p.supplier_name ILIKE %(proveedor)s"
            params["proveedor"] = f"%{proveedor}%"
        if min_relevancia:
            fallback_sql += " AND p.relevancia >= %(min_relevancia)s"
            params["min_relevancia"] = min_relevancia

        fallback_sql += """
        ORDER BY 
            p.relevancia DESC NULLS LAST,
            similarity_score DESC,
            b.name NULLS LAST,
            p.name 
        LIMIT %(limit)s
        """
        
        params.update({
            "q": f"%{q}%",
            "q_exact": q,
            "limit": limit,
        })

        out: List[Dict[str, Any]] = []
        with _conn() as conn:
            with conn.cursor() as cur:
                try:
                    # Intentar función avanzada primero
                    cur.execute(sql, params)
                except Exception:
                    # Fallback a consulta manual
                    cur.execute(fallback_sql, params)
                
                for row in cur.fetchall():
                    (
                        pid,
                        daterium_id,
                        name,
                        description,
                        brand_name,
                        family_name,
                        supplier_name,
                        relevancia,
                        thumb_url,
                        image_url,
                        similarity_score,
                    ) = row
                    
                    out.append({
                        "id": daterium_id or pid,
                        "internal_id": pid,
                        "daterium_id": daterium_id,
                        "nombre": name,
                        "descripcion": description,
                        "marca": brand_name,
                        "familia": family_name,
                        "proveedor": supplier_name,
                        "relevancia": float(relevancia) if relevancia else None,
                        "similarity_score": float(similarity_score) if similarity_score else 0,
                        "thumb": thumb_url,
                        "img": image_url or thumb_url,
                        "url_ficha": _wp_ficha_url(daterium_id, pid),
                        "google_url": _google_url(name, supplier_name or brand_name),
                    })

        return {"ok": True, "total": len(out), "productos": out, "query": q}

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
    Ficha completa del producto con categorías AECOC, referencias y datos del proveedor
    """
    try:
        id_num = None
        try:
            id_num = int(id)
        except Exception:
            pass

        # Consulta principal del producto
        sql = """
        SELECT
            p.id AS pid,
            p.daterium_id,
            p.name,
            p.description,
            p.pvp,
            p.thumb_url,
            p.image_url,
            p.supplier_name,
            p.supplier_cif,
            p.relevancia,
            p.idcatalogo,
            b.name AS brand_name,
            b.logo_url AS brand_logo,
            f.name AS subfamily_name,
            pf.name AS family_name
        FROM products p
        LEFT JOIN brands b ON b.id = p.brand_id
        LEFT JOIN families f ON f.id = p.family_id
        LEFT JOIN families pf ON pf.id = f.parent_id
        WHERE
            (%(idnum)s IS NOT NULL AND p.daterium_id = %(idnum)s)
            OR
            (%(idnum)s IS NOT NULL AND p.id = %(idnum)s)
        LIMIT 1
        """
        
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
                    pvp,
                    thumb_url,
                    image_url,
                    supplier_name,
                    supplier_cif,
                    relevancia,
                    idcatalogo,
                    brand_name,
                    brand_logo,
                    subfamily_name,
                    family_name,
                ) = row

                # Obtener imágenes
                cur.execute(
                    "SELECT url, is_primary FROM product_images WHERE product_id = %s ORDER BY is_primary DESC, id",
                    (pid,),
                )
                imgs = [{"url": u, "is_primary": bool(ip)} for (u, ip) in cur.fetchall()]

                # Obtener categorías AECOC
                cur.execute("""
                    SELECT ac.aecoc_id, ac.name, ac.level
                    FROM product_aecoc pa
                    JOIN aecoc_categories ac ON ac.id = pa.aecoc_id
                    WHERE pa.product_id = %s
                    ORDER BY ac.level
                """, (pid,))
                aecoc_categories = [
                    {"aecoc_id": aid, "name": aname, "level": level}
                    for (aid, aname, level) in cur.fetchall()
                ]

                # Obtener referencias adicionales si existen
                cur.execute("""
                    SELECT reference_code, reference_type, supplier_name, is_primary
                    FROM product_references
                    WHERE product_id = %s
                    ORDER BY is_primary DESC, reference_type
                """, (pid,))
                references = [
                    {
                        "code": code,
                        "type": ref_type,
                        "supplier": ref_supplier,
                        "is_primary": bool(is_primary)
                    }
                    for (code, ref_type, ref_supplier, is_primary) in cur.fetchall()
                ]

        # Construir respuesta completa
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
            "proveedor": {
                "nombre": supplier_name,
                "cif": supplier_cif,
            } if supplier_name else None,
            "pvp": float(pvp) if pvp is not None else None,
            "relevancia": float(relevancia) if relevancia is not None else None,
            "idcatalogo": idcatalogo,
            "thumb": thumb_url,
            "img": image_url or (imgs[0]["url"] if imgs else None),
            "imagenes": imgs,
            "aecoc_categories": aecoc_categories,
            "referencias_adicionales": references,
            "url_ficha_wp": _wp_ficha_url(daterium_id, pid),
            "google_url": _google_url(name, supplier_name or brand_name),
        }
        
        return ficha

    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Error en ficha: {ex}")

# ----------------- /stats -----------------
@router.get("/stats")
def get_stats():
    """Estadísticas del catálogo"""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                stats = {}
                
                # Conteos básicos
                cur.execute("SELECT COUNT(*) FROM products")
                stats["total_products"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM brands")
                stats["total_brands"] = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM families")
                stats["total_families"] = cur.fetchone()[0]
                
                # Top proveedores
                cur.execute("""
                    SELECT supplier_name, COUNT(*) as productos
                    FROM products 
                    WHERE supplier_name IS NOT NULL
                    GROUP BY supplier_name
                    ORDER BY productos DESC
                    LIMIT 10
                """)
                stats["top_suppliers"] = [
                    {"nombre": name, "productos": count}
                    for (name, count) in cur.fetchall()
                ]
                
                # Top marcas
                cur.execute("""
                    SELECT b.name, COUNT(*) as productos
                    FROM products p
                    JOIN brands b ON b.id = p.brand_id
                    GROUP BY b.name
                    ORDER BY productos DESC
                    LIMIT 10
                """)
                stats["top_brands"] = [
                    {"nombre": name, "productos": count}
                    for (name, count) in cur.fetchall()
                ]
                
                # Distribución por relevancia
                cur.execute("""
                    SELECT 
                        CASE 
                            WHEN relevancia >= 90 THEN 'Alta (90-100)'
                            WHEN relevancia >= 70 THEN 'Media (70-89)'
                            WHEN relevancia >= 50 THEN 'Baja (50-69)'
                            ELSE 'Sin clasificar'
                        END as rango,
                        COUNT(*) as productos
                    FROM products
                    GROUP BY 1
                    ORDER BY 2 DESC
                """)
                stats["relevancia_distribution"] = [
                    {"rango": rango, "productos": count}
                    for (rango, count) in cur.fetchall()
                ]
                
        return {"ok": True, "stats": stats}
        
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Error en estadísticas: {ex}")

# ----------------- /categorias_aecoc -----------------
@router.get("/categorias_aecoc")
def get_aecoc_categories(
    parent_id: Optional[str] = Query(None, description="ID de categoría padre"),
    limit: int = Query(50, ge=1, le=200)
):
    """Obtener categorías AECOC jerárquicas"""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                if parent_id:
                    # Obtener subcategorías
                    cur.execute("""
                        SELECT ac.aecoc_id, ac.name, ac.level, COUNT(pa.product_id) as product_count
                        FROM aecoc_categories ac
                        LEFT JOIN aecoc_categories parent ON parent.aecoc_id = %s
                        LEFT JOIN product_aecoc pa ON pa.aecoc_id = ac.id
                        WHERE ac.parent_id = parent.id
                        GROUP BY ac.aecoc_id, ac.name, ac.level
                        ORDER BY product_count DESC, ac.name
                        LIMIT %s
                    """, (parent_id, limit))
                else:
                    # Obtener categorías raíz
                    cur.execute("""
                        SELECT ac.aecoc_id, ac.name, ac.level, COUNT(pa.product_id) as product_count
                        FROM aecoc_categories ac
                        LEFT JOIN product_aecoc pa ON pa.aecoc_id = ac.id
                        WHERE ac.parent_id IS NULL
                        GROUP BY ac.aecoc_id, ac.name, ac.level
                        ORDER BY product_count DESC, ac.name
                        LIMIT %s
                    """, (limit,))
                
                categories = [
                    {
                        "aecoc_id": aecoc_id,
                        "name": name,
                        "level": level,
                        "product_count": product_count
                    }
                    for (aecoc_id, name, level, product_count) in cur.fetchall()
                ]
                
        return {"ok": True, "categories": categories, "parent_id": parent_id}
        
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Error en categorías AECOC: {ex}")