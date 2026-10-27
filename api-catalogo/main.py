from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI(title="API Catálogo DataMart")

# productos de prueba tomados de el csv
BASE_DATOS_PRODUCTOS = [
    {"codigo": "85123A", "nombre": "WHITE HANGING HEART T-LIGHT HOLDER", "categoria": "Hogar", "pais_origen": "China", "activo": True},
    {"codigo": "71053", "nombre": "WHITE METAL LANTERN", "categoria": "Hogar", "pais_origen": "India", "activo": True},
    {"codigo": "84406B", "nombre": "CREAM CUPID HEARTS COAT HANGER", "categoria": "Ropa", "pais_origen": "Reino Unido", "activo": False},
    {"codigo": "84029G", "nombre": "KNITTED UNION FLAG HOT WATER BOTTLE", "categoria": "Hogar", "pais_origen": "Reino Unido", "activo": True},
    {"codigo": "35004C", "nombre": "SET OF 3 COLOURED  FLYING DUCKS", "categoria": "Hogar", "pais_origen": "China", "activo": True}
]

# primer endpoint un get con paginacion
@app.get("/api/productos")
def obtener_productos(skip: int = Query(0, description="Registros a omitir"), 
                      limit: int = Query(10, description="Límite de registros")):
    productos_paginados = BASE_DATOS_PRODUCTOS[skip : skip + limit]
    return {
        "total": len(BASE_DATOS_PRODUCTOS),
        "skip": skip,
        "limit": limit,
        "data": productos_paginados
    }

# segundo endpoint un get por codigo de producto
@app.get("/api/productos/{codigo}")
def obtener_producto(codigo: str):
    # Buscamos el producto convirtiendo todo a mayúsculas para evitar errores de case sensitive
    for producto in BASE_DATOS_PRODUCTOS:
        if producto["codigo"].upper() == codigo.upper():
            return producto
    
    # Si no existe, devolvemos un error 404
    raise HTTPException(status_code=404, detail="Producto no encontrado en el catálogo")