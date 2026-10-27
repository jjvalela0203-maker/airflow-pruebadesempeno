import requests
import pandas as pd

print("1. Intentando conectar a la API...")
url_api = "http://localhost:8000/api/productos?skip=0&limit=10"

try:
    respuesta = requests.get(url_api)
    
    if respuesta.status_code == 200:
        print("¡Conexión exitosa!\n")
        

        json_data = respuesta.json()
        print(f"Total de productos reportados por la API: {json_data['total']}")
        
        df_catalogo = pd.DataFrame(json_data["data"])
        
        print("\n2. DataFrame generado desde la API:")
        print(df_catalogo[['codigo', 'nombre', 'categoria']])
    else:
        print(f"Error en la API. Código de estado: {respuesta.status_code}")

except requests.exceptions.ConnectionError:
    print("Error Fatal: No se pudo conectar. ¿Seguro que el contenedor de la API está corriendo en el puerto 8000?")