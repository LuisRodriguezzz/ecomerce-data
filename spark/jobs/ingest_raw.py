from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import datetime
import os

def get_spark_session():
    # Configuración CLAVE para que Spark hable con MinIO (S3)
    return SparkSession.builder \
        .appName("IngestionRaw") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def ingest_data():
    spark = get_spark_session()
    
    # 1. Simulación de datos (Aquí normalmente harías un request a una API)
    # Imaginemos que descargamos esto de una API de Ecommerce
    data = [
        (1, "ORD-001", "user_1", 150.50, datetime.datetime.now()),
        (2, "ORD-002", "user_2", 200.00, datetime.datetime.now()),
        (3, "ORD-003", "user_1", 50.25, datetime.datetime.now())
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    
    print("--- Datos Extraídos (Preview) ---")
    df.show()

    # 2. Definir ruta en MinIO (Bucket 'raw-data' que creó el minio-init)
    # Usamos formato 'parquet' que es más eficiente
    output_path = "s3a://raw-data/orders/"
    
    # 3. Escribir datos (Mode 'append' para no borrar lo anterior)
    print(f"--- Escribiendo en {output_path} ---")
    df.write.mode("append").parquet(output_path)
    
    print("--- Ingestión Exitosa ---")
    spark.stop()

if __name__ == "__main__":
    ingest_data()