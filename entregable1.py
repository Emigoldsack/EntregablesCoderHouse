import pandas as pd
import json
import requests
import psycopg2
from datetime import datetime

# URL de la API
url = 'https://www.dolarsi.com/api/api.php?type=valoresprincipales'

# Consulta SQL como constante
CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS cotizaciones (
        nombre VARCHAR(255),
        compra NUMERIC,
        venta NUMERIC,
        fecha DATE,
        PRIMARY KEY (nombre, fecha)
    )
"""

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

        # Cargar credenciales desde el archivo de configuración
        with open("config.json", "r") as config_file:
            config = json.load(config_file)

        # Crear una conexión a Redshift utilizando psycopg2
        conn = psycopg2.connect(
            dbname=config['db_name'],
            user=config['db_user'],
            password=config['db_password'],
            host=config['db_host'],
            port=config['db_port']
        )

        # Crear un cursor para ejecutar consultas
        cursor = conn.cursor()

        # Ejecutar la consulta SQL para crear la tabla
        cursor.execute(CREATE_TABLE_QUERY)

        # Confirmar la creación de la tabla
        conn.commit()

        # Obtener la fecha actual
        fecha_actual = datetime.now().date()

        
        # Reemplazar comas por puntos en las columnas 'compra' y 'venta'
        df['compra'] = df['compra'].str.replace(',', '.')
        df['venta'] = df['venta'].str.replace(',', '.')

        # Reemplazar 'No Cotiza' por 0 en las columnas 'compra' y 'venta'
        df['compra'] = df['compra'].replace('No Cotiza', '0')
        df['venta'] = df['venta'].replace('No Cotiza', '0')

        # Convertir las columnas a tipo float
        df['compra'] = df['compra'].astype(float)
        df['venta'] = df['venta'].astype(float)

        # Cargar los datos en Redshift con la fecha actual
        for index, row in df.iterrows():
         cursor.execute(
          "INSERT INTO cotizaciones (nombre, compra, venta, fecha) VALUES (%s, %s, %s, %s)",
        (row['nombre'], row['compra'], row['venta'], fecha_actual)
        )

        # Confirmar la inserción de los datos
        conn.commit()

        # Cerrar el cursor y la conexión
        cursor.close()
        conn.close()

        print("Datos cargados exitosamente en la base de datos.")
    else:
        print(f'Error en la solicitud: {response.status_code}')
except Exception as e:
    print(f'Error al conectarse a la API o a la base de datos: {str(e)}')