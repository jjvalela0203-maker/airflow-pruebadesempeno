# Data Warehouse & ETL Pipeline - DataSmart S.A.S

Este repositorio contiene la solución técnica para el desafío de Data Engineering de DataSmart S.A.S. El proyecto consiste en un pipeline ETL automatizado con **Apache Airflow** que extrae datos de ventas transaccionales, los enriquece consumiendo una **API REST (FastAPI)**, los transforma utilizando **Pandas** y los carga de manera idempotente en un Data Warehouse construido sobre **PostgreSQL**.

Todo el entorno está orquestado mediante contenedores de **Docker** para garantizar su portabilidad y fácil despliegue.

---

## 🛠️ Arquitectura y Tecnologías

* **Orquestación:** Apache Airflow (optimizado con LocalExecutor).
* **Procesamiento:** Python (Pandas, Requests, SQLAlchemy).
* **Data Warehouse:** PostgreSQL 13.
* **API Mock:** FastAPI.
* **Contenedorización:** Docker & Docker Compose.

---

## ⚙️ Instrucciones de Despliegue (Setup)

Sigue estos pasos para levantar el proyecto en cualquier entorno local:

1. **Clonar el repositorio:**

   ```bash
   git clone https://github.com/jjvalela0203-maker/airflow-pruebadesempeno
   cd aiflow-pruebdesempeño
   ```
2. **Configurar variables de entorno:**
   Copia la plantilla `.env.example` y renómbrala a `.env`. (Las credenciales por defecto ya están configuradas para el entorno de desarrollo).
3. **Levantar la infraestructura con Docker:**

   ```bash
   docker compose up -d --build
   ```
4. **Accesos del sistema:**

   * **Airflow Web UI:** `http://localhost:8080` (Usuario: `airflow` / Clave: `airflow`)
   * **Base de Datos (DBeaver):** `localhost:5433` (Mapeado al puerto interno 5432)
   * **API Catálogo:** `http://localhost:8000/docs`
5. **Ejecutar el Pipeline:**
   Ingresa a la interfaz de Airflow, activa el DAG `datamart_etl_pipeline` y presiona *Trigger DAG*.

---

## 🧠 Decisiones Técnicas y de Negocio

Para asegurar la calidad, eficiencia y consistencia de los datos, se tomaron las siguientes decisiones de ingeniería durante el desarrollo del pipeline:

### 1. Optimización del Motor de Airflow (LocalExecutor)

Por defecto, la imagen de Airflow utiliza `CeleryExecutor`, lo que levanta múltiples servicios pesados (Redis, Workers, Flower) innecesarios para un entorno de una sola máquina. Se modificó el `docker-compose.yaml` para utilizar **`LocalExecutor`**, reduciendo drásticamente el consumo de memoria RAM y acelerando los tiempos de despliegue sin perder capacidades de paralelismo local.

### 2. Estrategia de Idempotencia en la Carga de Datos

El pipeline fue diseñado para ser 100% idempotente (puede ejecutarse múltiples veces sin duplicar información). Para lograrlo, en lugar de usar la función `replace` de Pandas (que destruye y recrea la tabla borrando llaves primarias y tipos de dato), se utilizó un enfoque transaccional:

* Se ejecuta un `TRUNCATE TABLE ... RESTART IDENTITY CASCADE` mediante `SQLAlchemy (engine.begin())` para limpiar los datos existentes justo antes de la carga.
* Posteriormente, se utiliza el método `if_exists='append'` de Pandas para insertar los datos respetando estrictamente el esquema DDL original diseñado en la base de datos.

### 3. Filtros de Limpieza (Data Quality)

* **Precios Anómalos:** Cualquier registro con un `UnitPrice` menor o igual a `0` fue catalogado como error de sistema y filtrado hacia una tabla de auditoría llamada `log_rechazos`.
* **Registros duplicados:** Se aplicó `drop_duplicates()` para evitar procesar la misma transacción dos veces.

### 4. Separación de Ventas y Devoluciones

Se decidió separar físicamente las ventas efectivas de las devoluciones en dos tablas distintas (`ventas` y `devoluciones`).

* **Criterio de separación:** Se utilizó un criterio estrictamente matemático basado en la cantidad (`Quantity > 0` para ventas y `Quantity <= 0` para devoluciones). **No se utilizó la letra 'C' en el InvoiceNo** como criterio principal porque el análisis exploratorio reveló que existían inconsistencias en los datos donde algunas devoluciones no estaban marcadas con dicha letra.

### 5. Cálculo del Revenue Neto

El campo `netrevenue` se calculó multiplicando la cantidad de artículos por el precio unitario (`Quantity * UnitPrice`).

* *Nota analítica:* En la tabla de `devoluciones`, la cantidad venía en negativo. Para poder totalizar el dinero devuelto sin cruzar signos, se aplicó un valor absoluto (`abs()`) al calcular el revenue de los retornos.

### 6. Integración con la API y Manejo de Nulos

Se consumió la API de catálogo utilizando paginación. Los datos extraídos se cruzaron con las transacciones usando un `Left Join`. Los productos transaccionales cuyo `StockCode` no existía en la respuesta de la API fueron identificados y su columna `categoría` fue imputada con el valor por defecto `"Sin Categoria"` para mantener la integridad de las consultas de agrupación (`GROUP BY`).

---

## 📊 Consultas de Negocio y Modelo de Datos

* **Consultas SQL:** Las respuestas a las 7 preguntas de negocio solicitadas se encuentran en el archivo `consultas/querys.sql`.
