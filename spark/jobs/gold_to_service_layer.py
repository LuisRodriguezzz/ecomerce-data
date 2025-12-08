from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round, when
import os

def get_spark_session():
    return SparkSession.builder \
        .appName("Publish_Gold_to_Postgres") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def process():
    spark = get_spark_session()
    
    # 1. Leer GOLD desde MinIO
    input_path = "s3a://processed-data/gold/hourly_kpis/"
    print(f"--- [PUBLISH] Leyendo Gold desde {input_path} ---")
    df_gold = spark.read.format("delta").load(input_path)
    
    # 2. Configurar Conexión a 'analytics_db' (Tu base propia)
    jdbc_url = "jdbc:postgresql://postgres:5432/analytics_db" 
    
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    
    # --- ESTRATEGIA DE CUBO / STAR SCHEMA ---
    
    # TABLA 1: HECHOS DETALLADOS (Fact Table)
    # Esta es la tabla principal con todo el detalle por hora.
    print("--- [PUBLISH] Escribiendo Tabla de Hechos (Hourly) ---")
    df_gold.write \
        .jdbc(url=jdbc_url, table="public.fact_hourly_sales", mode="overwrite", properties=properties)

    # TABLA 2: AGREGACIÓN DIARIA (Rollup)
    # Simulamos un "Cubo" pre-calculando los totales por día.
    # Power BI usará esto para gráficos de tendencias anuales (más rápido que sumar horas).
    print("--- [PUBLISH] Calculando y Escribiendo Agregación Diaria ---")
    
    df_daily = df_gold.groupBy("event_date").agg(
        sum("total_revenue").alias("total_revenue"),
        sum("cnt_purchase").alias("total_orders"),
        sum("unique_visitors").alias("total_visitors")
    ).orderBy("event_date")
    
    # Recalculamos métricas complejas (No se pueden sumar promedios, hay que recalcularlos)
    df_daily = df_daily.withColumn("conversion_rate", 
        when(col("total_visitors") > 0, 
             round((col("total_orders") / col("total_visitors")) * 100, 2)).otherwise(0)
    )

    df_daily.write \
        .jdbc(url=jdbc_url, table="public.agg_daily_summary", mode="overwrite", properties=properties)

    # TABLA 3: DIMENSIÓN CATEGORÍA
    # Tabla única de categorías para usar en filtros (slicers)
    print("--- [PUBLISH] Escribiendo Dimensión Categoría ---")
    
    df_category = df_gold.select("main_category").distinct().orderBy("main_category")
    
    df_category.write \
        .jdbc(url=jdbc_url, table="public.dim_category", mode="overwrite", properties=properties)

    print("--- [PUBLISH] Carga Exitosa en 'analytics_db'. Cubo listo. ---")
    spark.stop()

if __name__ == "__main__":
    process()