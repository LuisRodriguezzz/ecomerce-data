from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, split, hour, dayofweek, when, lower, trim, lit
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
    spark = get_spark_session() # (Asumimos tu función get_spark_session de siempre)
    
    input_path = "s3a://raw-data/bronze/cosmetics_events_delta/"
    output_path = "s3a://processed-data/silver/cosmetics_events/"
    
    df = spark.read.format("delta").load(input_path)
    
    # --- 1. LIMPIEZA TÉCNICA Y FILTRADO (Lo que ya tenías) ---
    df_clean = df \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("user_id", col("user_id").cast("long")) \
        .filter(col("user_id").isNotNull()) \
        .filter(col("price") >= 0) 

    # --- 2. ENRIQUECIMIENTO Y ESTANDARIZACIÓN (NUEVO) ---
    
    df_enriched = df_clean \
        .withColumn("brand", lower(trim(col("brand")))) \
        .withColumn("event_type", lower(trim(col("event_type")))) \
        \
        .fillna({'brand': 'unknown', 'category_code': 'unknown'}) \
        \
        .withColumn("main_category", split(col("category_code"), "\.").getItem(0)) \
        .withColumn("sub_category", split(col("category_code"), "\.").getItem(1)) \
        \
        .withColumn("hour", hour(col("event_time"))) \
        .withColumn("is_weekend", when(dayofweek(col("event_time")).isin([1, 7]), True).otherwise(False)) \
        \
        .withColumn("price_segment", \
            when(col("price") < 5, "Low Cost") \
            .when((col("price") >= 5) & (col("price") < 20), "Mid Range") \
            .otherwise("Premium") \
        )

    # --- 3. ESCRITURA ---
    df_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    spark.stop()

if __name__ == "__main__":
    process()