# Utiliza una imagen base de Python
FROM python:3.8

# Instala Apache Airflow con pip
RUN pip install apache-airflow

# Crea una carpeta para los archivos DAG y establece esta carpeta como directorio de trabajo
WORKDIR /usr/local/airflow

# Copia el archivo de tu DAG a la carpeta de DAGs en el contenedor
COPY mi_dag.py /usr/local/airflow/dags/

# Inicia Apache Airflow cuando se ejecute el contenedor
CMD ["airflow", "webserver"]
