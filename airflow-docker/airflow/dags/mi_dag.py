from datetime import datetime, timedelta
import json
import requests
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

# Leer los datos de conexión desde el archivo JSON que esta en carpeta "config"
with open('/Users/Mariela/Desktop/Coder House 2023/PrimerEntregableCoderHouse/airflow-docker/config/config.json', 'r') as config_file:
    config_data = json.load(config_file)

# Obtener los valores de conexión desde el diccionario
redshift_host = config_data['db_host']
redshift_port = config_data['db_port']
redshift_database = config_data['db_name']
redshift_user = config_data['db_user']
redshift_password = config_data['db_password']

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

# Tarea para crear la tabla en la base de datos Redshift (con IF NOT EXISTS)
crear_tabla_task = PostgresOperator(
    task_id='crear_tabla',
    postgres_conn_id='redshift_conn',  
    sql=f"""
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

            # Persistir los datos en disco como un archivo CSV
            df.to_csv('/tmp/cotizaciones.csv', index=False)

            return '/tmp/cotizaciones.csv'
        else:
            return None
    except Exception as e:
        return None

# Función para cargar datos desde el archivo CSV
def cargar_datos_desde_csv(csv_path):
    try:
        # Leer los datos desde el archivo CSV
        df = pd.read_csv(csv_path)

        # Realizar la carga de datos en Redshift
        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            database=redshift_database,
            user=redshift_user,
            password=redshift_password
        )

        cur = conn.cursor()
        for index, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO cotizaciones (nombre, compra, venta, fecha)
                VALUES (%s, %s, %s, %s)
                """,
                (row['nombre'], row['compra'], row['venta'], row['fecha'])
            )
        conn.commit()
        cur.close()
        conn.close()

        return True
    except Exception as e:
        return False

# Tarea para cargar cotizaciones utilizando PythonOperator
cargar_cotizaciones_task = PythonOperator(
    task_id='cargar_cotizaciones',
    python_callable=cargar_cotizaciones,
    dag=dag,
)

# Tarea para cargar datos desde el archivo CSV
cargar_datos_desde_csv_task = PythonOperator(
    task_id='cargar_datos_desde_csv',
    python_callable=cargar_datos_desde_csv,
    op_args=['{{ ti.xcom_pull(task_ids="cargar_cotizaciones") }}'],  # Obtener el resultado de la tarea anterior
    provide_context=True,
    dag=dag,
)

# Definir la secuencia de tareas
crear_tabla_task >> cargar_cotizaciones_task >> cargar_datos_desde_csv_task
