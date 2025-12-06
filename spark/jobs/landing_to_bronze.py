from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
import os

def get_spark_session():
    return SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def process():
    spark = get_spark_session()
    
    # Rutas
    input_path = "s3a://raw-data/landing/*.csv" 
    # OJO: Ahora guardamos en una carpeta 'delta' para diferenciar
    output_path = "s3a://raw-data/bronze/cosmetics_events_delta/"
    
    print(f"--- Leyendo CSVs desde {input_path} ---")
    
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(input_path)
    
    df_transformed = df \
        .withColumn("ingestion_date", current_timestamp()) \
        .withColumn("source_file", input_file_name())

    print(f"--- Escribiendo DELTA TABLE en {output_path} ---")
    
    # --- CAMBIO AQUÍ: format("delta") ---
    df_transformed.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)
    
    print("--- Transformación a Delta Lake Finalizada ---")
    spark.stop()

if __name__ == "__main__":
    process()