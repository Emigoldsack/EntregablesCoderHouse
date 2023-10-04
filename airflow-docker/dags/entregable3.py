from datetime import datetime, timedelta
import json
import requests
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

# Definir los argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crear el objeto DAG
dag = DAG(
    'mi_dag',
    default_args=default_args,
    description='Un DAG que obtiene cotizaciones y carga datos en Redshift',
    schedule_interval=timedelta(days=1),
)

# Función para cargar cotizaciones en la base de datos
def cargar_cotizaciones():
    # URL de la API
    url = 'https://www.dolarsi.com/api/api.php?type=valoresprincipales'
    
    try:
        # Realizar la solicitud HTTP a la API
        response = requests.get(url)

        # Verificar si la solicitud fue exitosa 
        if response.status_code == 200:
            # Obtener los datos JSON
            cotizaciones = response.json()

            # Filtrar las cotizaciones para excluir ciertas monedas
            cotizaciones_filtradas = [casa['casa'] for casa in cotizaciones if casa['casa']['nombre'] not in ['Argentina', 'Bitcoin', 'Dolar Soja']]

            # Crear un DataFrame con los datos
            df = pd.DataFrame(cotizaciones_filtradas)

            return df
        else:
           
            return None
    except Exception as e:
        
        return None

# Tarea para cargar cotizaciones utilizando PythonOperator
cargar_cotizaciones_task = PythonOperator(
    task_id='cargar_cotizaciones',
    python_callable=cargar_cotizaciones,
    dag=dag,
)

# Tarea para crear la tabla en la base de datos Redshift
crear_tabla_task = PostgresOperator(
    task_id='crear_tabla',
    postgres_conn_id='redshift_conn',  
    sql="""
        CREATE TABLE IF NOT EXISTS cotizaciones (
            nombre VARCHAR(255),
            compra NUMERIC,
            venta NUMERIC,
            fecha DATE,
            PRIMARY KEY (nombre, fecha),
            CONSTRAINT unique_nombre_fecha UNIQUE (nombre, fecha)
        )
    """,
    dag=dag,
)

# Tarea para cargar datos en la base de datos Redshift
cargar_datos_task = PostgresOperator(
    task_id='cargar_datos',
    postgres_conn_id='redshift_conn',  
    sql="""
        INSERT INTO cotizaciones (nombre, compra, venta, fecha)
        VALUES (%s, %s, %s, %s)
    """,
    depends_on_past=False,
    dag=dag,
)

# Definir la secuencia de tareas
cargar_cotizaciones_task >> crear_tabla_task >> cargar_datos_task
