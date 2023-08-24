from datetime import datetime
import json
import requests
import psycopg2

# URL de la API
url = 'https://www.dolarsi.com/api/api.php?type=valoresprincipales'

try:
    # Realizar la solicitud HTTP a la API
    response = requests.get(url)

    # Verificar si la solicitud fue exitosa (código de respuesta 200)
    if response.status_code == 200:
        # Obtener los datos JSON
        cotizaciones = response.json()

        # Filtrar las cotizaciones para excluir ciertas monedas
        cotizaciones_filtradas = [casa for casa in cotizaciones if casa['casa']['nombre'] not in ['Argentina', 'Bitcoin', 'Dolar Soja']]

        # Cargar credenciales desde el archivo de configuración
        with open("config.json", "r") as config_file:
          config = json.load(config_file)

        db_host = config["db_host"]
        db_port = config["db_port"]
        db_name = config["db_name"]
        db_user = config["db_user"]
        db_password = config["db_password"]

        # Conectar a la base de datos usando las credenciales obtenidas
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        # Crear un cursor
        cursor = conn.cursor()

        for cotizacion in cotizaciones_filtradas:
            nombre = cotizacion['casa']['nombre']
            compra = cotizacion['casa']['compra']
            venta = cotizacion['casa']['venta']

            # Modificar los valores numéricos con comas por puntos
            if compra != 'No Cotiza':
                compra = compra.replace(',', '.')
            else:
                compra = '0'  # Valor predeterminado para 'No Cotiza'

            if venta != 'No Cotiza':
                venta = venta.replace(',', '.')
            else:
                venta = '0'  # Valor predeterminado para 'No Cotiza'

            # Obtener la fecha y hora actual
            fecha = datetime.now()

            # Comprobar si el registro ya existe en la tabla
            cursor.execute("SELECT COUNT(*) FROM cotizaciones WHERE nombre = %s", (nombre,))
            existe_registro = cursor.fetchone()[0]

            if existe_registro > 0:
                # Si el registro existe, actualizarlo en lugar de insertarlo
                cursor.execute("UPDATE cotizaciones SET compra = %s, venta = %s, fecha = %s WHERE nombre = %s", (compra, venta, fecha, nombre))
            else:
                # Si el registro no existe, insertarlo
                cursor.execute("INSERT INTO cotizaciones (nombre, compra, venta, fecha) VALUES (%s, %s, %s, %s)", (nombre, compra, venta, fecha))

        # Commit y cerrar la conexión
        conn.commit()
        conn.close()
    else:
        print(f'Error en la solicitud: {response.status_code}')
except Exception as e:
    print(f'Error al conectarse a la API o a Redshift: {str(e)}')

