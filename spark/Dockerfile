FROM python:3.10-slim-bookworm

# Instalar Java (OpenJDK) y utilidades necesarias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre \
    curl \
    wget \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configurar variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Definir versiones de Spark y Hadoop
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

# Descargar y configurar Apache Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Configurar PATH para Spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3

# Descargar JARs necesarios para Delta Lake y S3
RUN wget -P ${SPARK_HOME}/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Instalar librerías de Python
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    delta-spark==3.0.0 \
    pandas \
    minio \
    pyarrow \
    fastparquet \
    boto3

# Crear directorios de trabajo
RUN mkdir -p /opt/spark-app /opt/data/duckdb /opt/spark/logs

# Configurar Spark para trabajar con S3 (MinIO)
RUN echo "spark.jars.packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.endpoint http://minio:9000" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.access.key admin" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.secret.key minioadmin" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.path.style.access true" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.connection.ssl.enabled false" >> ${SPARK_HOME}/conf/spark-defaults.conf

# Directorio de trabajo
WORKDIR /opt/spark-app

# Exponer puertos
EXPOSE 8080 7077 6066 4040

# El comando se definirá en docker-compose.yml