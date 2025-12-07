from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, split, hour, dayofweek, when, lit
import os

def get_spark_session():
    return SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def process():
    spark = get_spark_session()
    
    input_path = "s3a://raw-data/bronze/cosmetics_events_delta/"
    output_path = "s3a://processed-data/silver/cosmetics_events/"
    
    print(f"--- [SILVER] Leyendo datos desde {input_path} ---")
    df = spark.read.format("delta").load(input_path)
    
    print("--- [SILVER] Enriqueciendo datos (Feature Engineering) ---")
    
    # 1. Limpieza Básica y Tipado (Lo que ya teníamos)
    df_step1 = df \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("user_id", col("user_id").cast("long")) \
        .filter(col("user_id").isNotNull()) \
        .filter(col("price") >= 0)

    # 2. VARIABLES CALCULADAS (NUEVO)
    
    # A) Separar Categorías (ej: "electronics.audio" -> "electronics", "audio")
    # Usamos split por el punto. getItem(0) es la primera parte.
    df_enriched = df_step1 \
        .withColumn("main_category", split(col("category_code"), "\.").getItem(0)) \
        .withColumn("sub_category", split(col("category_code"), "\.").getItem(1)) \
        
    # B) Variables Temporales
    # dayofweek: 1=Domingo, 7=Sábado (en Spark por defecto)
    df_enriched = df_enriched \
        .withColumn("hour", hour(col("event_time"))) \
        .withColumn("is_weekend", when(dayofweek(col("event_time")).isin([1, 7]), True).otherwise(False))
    
    # C) Franja Horaria (Lógica de Negocio)
    # Madrugada: 0-6, Mañana: 6-12, Tarde: 12-18, Noche: 18-24
    df_enriched = df_enriched.withColumn("time_of_day", \
        when((col("hour") >= 0) & (col("hour") < 6), "Madrugada") \
        .when((col("hour") >= 6) & (col("hour") < 12), "Mañana") \
        .when((col("hour") >= 12) & (col("hour") < 18), "Tarde") \
        .otherwise("Noche") \
    )

    print("--- [SILVER] Esquema Enriquecido ---")
    df_enriched.printSchema()
    
    print(f"--- [SILVER] Escribiendo en {output_path} ---")
    
    # Escribimos particionado (Partitioning)
    # mergeSchema="true" es vital si agregamos columnas nuevas a una tabla Delta existente
    df_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    print("--- [SILVER] Proceso Finalizado ---")
    spark.stop()

if __name__ == "__main__":
    process()