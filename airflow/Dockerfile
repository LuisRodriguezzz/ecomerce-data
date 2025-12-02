FROM apache/airflow:2.7.1

# 1. Cambiar temporalmente al usuario ROOT para instalar dependencias del sistema.
USER root

# 2. Instalar dependencias del sistema operativo (apt-get)
# Ahora, como root, tienes los permisos para manipular /var/lib/apt/lists
RUN apt-get update && \
    apt-get install -y build-essential python3-dev libicu-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Volver al usuario por defecto de Airflow (airflow)
# Esto es CRUCIAL para mantener la seguridad y que Airflow pueda ejecutarse correctamente.
USER airflow 

# 4. Instalar las librerías de Python.
# pip generalmente puede instalar librerías en el entorno virtual del usuario Airflow.
RUN pip install --no-cache-dir pyspark pandas minio apache-airflow==2.7.1 apache-airflow-providers-apache-spark apache-airflow-providers-amazon