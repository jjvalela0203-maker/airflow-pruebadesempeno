import pandas as pd
import os
from datetime import datetime, timedelta
import requests
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

default_args = {
    'owner': 'ingeniero_junior',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # Ajustar a la fecha lógica de tus datos
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def extraer_y_transformar(**kwargs):
    print("Iniciando extracción y transformación...")
    api_limit = int(Variable.get("api_limite_paginacion", default_var=100))
    ruta_base = '/opt/airflow/dags/data/'
    
    url_api = f"http://api_catalogo:8000/api/productos?skip=0&limit={api_limit}"
    respuesta = requests.get(url_api)


    df1 = pd.read_csv(f'{ruta_base}data.csv', encoding='unicode_escape')

    ventas = (df1["Quantity"] > 0).sum()
    devoluciones = (df1["Quantity"] <= 0).sum()

    print(f"Total de ventas: {ventas}"
        f"\nTotal de devoluciones: {devoluciones}")

    precios_invalidas= df1[(df1["Quantity"] > 0) & (df1["UnitPrice"] <= 0)]

    print("Total de ventas con precio unitario inválido: ", len(precios_invalidas))
    print(precios_invalidas[["InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice"]].head(10))

    duplicados = df1.duplicated().sum()
    print("Total de registros duplicados: ", duplicados)


    nulos_cliente = df1['CustomerID'].isna().sum()
    porcentaje_nulos = (nulos_cliente / len(df1)) * 100

    print(f"Registros sin CustomerID: {nulos_cliente} ({porcentaje_nulos:.2f}% del total)")

    # Buscar códigos de producto que tengan múltiples descripciones únicas
    desc_por_codigo = df1.groupby('StockCode')['Description'].nunique()
    codigos_conflictivos = desc_por_codigo[desc_por_codigo > 1]

    print(f"Cantidad de StockCodes con más de una descripción: {len(codigos_conflictivos)}")

    # Ver un ejemplo específico si existen conflictos
    if len(codigos_conflictivos) > 0:
        ejemplo = codigos_conflictivos.index[0]
        print(f"\nVariaciones para el código '{ejemplo}':")
        print(df1[df1['StockCode'] == ejemplo]['Description'].unique())

    # Ver tipo de dato actual y rangos
    print("Tipo de dato de InvoiceDate:", df1['InvoiceDate'].dtype)
    df1['InvoiceDate'] = pd.to_datetime(df1['InvoiceDate'], errors='coerce', utc=True)
    print("Tipo de dato de InvoiceDate:", df1['InvoiceDate'].dtype)
    print("Fecha más antigua:", df1['InvoiceDate'].min())
    print("Fecha más reciente:", df1['InvoiceDate'].max())

    df2 = pd.read_csv(f'{ruta_base}online_retail.csv', encoding='unicode_escape')

    df1.columns = df1.columns.str.lower().str.replace(' ', '')
    df2.columns = df2.columns.str.lower().str.replace(' ', '')

    # 3. Fusionar ambos datasets verticalmente
    df_total = pd.concat([df1, df2], ignore_index=True)

    # 4. Resolver solapamiento (Duplicados entre fuentes)
    # Reasignamos en lugar de usar inplace=True
    duplicados_totales = df_total.duplicated().sum()
    df_total = df_total.drop_duplicates()

    print(f"Total de registros tras la fusión: {len(df_total)}")
    print(f"Duplicados eliminados por solapamiento: {duplicados_totales}")

    # transformaciones y limpieza de datos

    # rellenar el id del cliente
    df_total['customerid'] = df_total['customerid'].astype(object).fillna('Sin_Registro')
    df_total['description'] = df_total['description'].astype(object).fillna('SIN DESCRIPCION')

    # darle a las fechas formato utc
    df_total['invoicedate'] = pd.to_datetime(df_total['invoicedate'], errors='coerce', utc=True)

    # Normalizar textos[cite: 3, 4]
    df_total['stockcode'] = df_total['stockcode'].astype(str).str.upper().str.replace(' ', '')
    df_total['description'] = df_total['description'].astype(str).str.upper().str.strip()

    # extraemos el catálogo de productos desde la api
    datos_api = respuesta.json()["data"]

    df_catalogo = pd.DataFrame(datos_api)
    df_catalogo = df_catalogo[['codigo', 'categoria']] 

    df_catalogo['codigo'] = df_catalogo['codigo'].astype(str).str.upper()

    #DEVOLUCIONES: Cantidad menor o igual a cero
    df_devoluciones = df_total[df_total['quantity'] <= 0].copy()


    df_ventas_potenciales = df_total[df_total['quantity'] > 0]

    #LOG DE RECHAZOS: Ventas con precio unitario cero o negativo
    df_rechazos = df_ventas_potenciales[df_ventas_potenciales['unitprice'] <= 0].copy()
    df_rechazos['motivo'] = 'Precio unitario nulo o negativo'
    df_rechazos['fuente'] = 'validacion_pandas'
    df_rechazos.drop(columns=['index'], inplace=True)

    #VENTAS VÁLIDAS: Precio mayor a cero
    df_ventas = df_ventas_potenciales[df_ventas_potenciales['unitprice'] > 0].copy()


    df_ventas['bruterevenue'] = df_ventas['quantity'] * df_ventas['unitprice']
    df_ventas = df_ventas.merge(df_catalogo, left_on='stockcode', right_on='codigo', how='left')
    df_ventas['categoria'] = df_ventas['categoria'].fillna('Sin Categoria')
    df_ventas.drop(columns=['index'], inplace=True)
    df_ventas.drop(columns=['codigo'], inplace=True)

    df_ventas['fecha_dia'] = df_ventas['invoicedate'].dt.date
    df_devoluciones['fecha_dia'] = df_devoluciones['invoicedate'].dt.date

    # aca el abs es importante por que como son negativos necesitamos el valor absoluto para calcular el revenue neto correctamente
    df_devoluciones['valor_devuelto'] = df_devoluciones['quantity'].abs() * df_devoluciones['unitprice']


    devoluciones_diarias = df_devoluciones.groupby(['fecha_dia', 'stockcode'])['valor_devuelto'].sum().reset_index()

    df_ventas = df_ventas.merge(devoluciones_diarias, on=['fecha_dia', 'stockcode'], how='left')

    df_ventas.rename(columns={'categoria': 'category'}, inplace=True)
    
    # si el producto no tuvo devoluciones ese día, el merge deja un nan. eso lo rellenamos con 0.
    df_ventas['valor_devuelto'] = df_ventas['valor_devuelto'].fillna(0)
    df_ventas['netrevenue'] = df_ventas['bruterevenue'] - df_ventas['valor_devuelto']

    # se sacan columnas que no deben ir en la base de datos
    df_ventas.drop(columns=['fecha_dia', 'valor_devuelto'], inplace=True)
    df_devoluciones.drop(columns=['fecha_dia', 'valor_devuelto'], inplace=True)
    df_devoluciones.drop(columns=['index'], inplace=True)

    print("Cálculo de Revenue Neto completado con éxito.")
    print(df_ventas[['stockcode', 'quantity', 'bruterevenue', 'netrevenue']].head())


    df_ventas.to_csv(f'{ruta_base}ventas_limpias.csv', index=False, encoding='utf-8')
    df_devoluciones.to_csv(f'{ruta_base}devoluciones_limpias.csv', index=False, encoding='utf-8')
    df_rechazos.to_csv(f'{ruta_base}log_rechazos.csv', index=False, encoding='utf-8')

    print("¡Archivos de staging exportados exitosamente!")

def cargar_a_postgres(**kwargs):
    print("Iniciando carga a PostgreSQL...")
    ruta_base = '/opt/airflow/dags/data/'
    
    df_ventas = pd.read_csv(f'{ruta_base}ventas_limpias.csv')
    df_devoluciones = pd.read_csv(f'{ruta_base}devoluciones_limpias.csv')
    df_rechazos = pd.read_csv(f'{ruta_base}log_rechazos.csv')
    

    hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = hook.get_sqlalchemy_engine()
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE ventas, devoluciones, log_rechazos RESTART IDENTITY CASCADE;"))
    
    # Explicaremos cómo mejorar esto para que sea 100% idempotente
    df_ventas.to_sql('ventas', engine, if_exists='append', index=False)
    df_devoluciones.to_sql('devoluciones', engine, if_exists='append', index=False)
    df_rechazos.to_sql('log_rechazos', engine, if_exists='append', index=False)
    
    print("Carga a PostgreSQL completada.")

with DAG(
    'datamart_etl_pipeline',
    default_args=default_args,
    description='Pipeline de extracción, transformación y carga para DataMart',
    schedule_interval='@daily', # Ejecución diaria
    catchup=False,
    tags=['datamart', 'etl']
) as dag:

    tarea_transformar = PythonOperator(
        task_id='extraer_y_transformar',
        python_callable=extraer_y_transformar,
        provide_context=True
    )

    tarea_cargar = PythonOperator(
        task_id='cargar_a_postgres',
        python_callable=cargar_a_postgres,
        provide_context=True
    )

    # Definir el orden de ejecución
    tarea_transformar >> tarea_cargar