from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import os

def get_spark_session():
    return SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def process():
    spark = get_spark_session()
    
    input_path = "s3a://raw-data/landing/*.csv" 
    output_path = "s3a://raw-data/bronze/cosmetics_events_delta/"
    
    # --- DEFINICIÓN DE ESQUEMA (SCHEMA ENFORCEMENT) ---
    # Esto es mucho más rápido y seguro que inferSchema
    custom_schema = StructType([
        StructField("event_time", StringType(), True),      # Leemos como String primero por seguridad
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),        # IDs suelen ser números largos
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),           # Precio siempre Double
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True)
    ])
    
    print(f"--- Leyendo CSVs con Esquema Estricto desde {input_path} ---")
    
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(custom_schema) \
        .load(input_path)  # <-- Aquí Spark ya sabe qué esperar, no adivina
    
    print("--- Esquema Validado ---")
    df.printSchema()
    
    # Transformaciones de Metadatos
    df_transformed = df \
        .withColumn("ingestion_date", current_timestamp()) \
        .withColumn("source_file", input_file_name())

    print(f"--- Escribiendo DELTA en {output_path} ---")
    
    # Guardamos en Bronze
    df_transformed.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)
    
    print("--- Transformación Bronze Finalizada ---")
    spark.stop()

if __name__ == "__main__":
    process()