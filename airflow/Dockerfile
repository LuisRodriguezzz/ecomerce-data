FROM apache/airflow:2.7.1-python3.10

USER root

# Instalamos Java y dependencias del sistema
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    python3-dev \
    libicu-dev \
    openjdk-17-jre-headless \
    procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configuramos JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow 

# Actualizamos pip
RUN pip install --upgrade pip

# --- BLOQUE CORREGIDO ---
# Instalamos TODO en una sola línea para que el resolver de dependencias funcione bien.
# Fijamos pyspark==3.5.0 para que NO intente bajar la 4.0.1
# Usamos timeout alto para que no falle tu internet.
RUN pip install --no-cache-dir --default-timeout=1000 \
    pyspark==3.5.0 \
    pandas \
    minio \
    apache-airflow==2.7.1 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    kaggle