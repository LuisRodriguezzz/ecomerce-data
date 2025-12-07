from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, countDistinct, when, round, desc
import os

def get_spark_session():
    return SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
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
    
    # Rutas
    input_path = "s3a://processed-data/silver/cosmetics_events/"
    output_path = "s3a://processed-data/gold/daily_kpis/"
    
    print(f"--- [GOLD] Leyendo Silver desde {input_path} ---")
    df = spark.read.format("delta").load(input_path)
    
    print("--- [GOLD] Calculando KPIs de Negocio ---")
    
    # --- LA LÓGICA DE NEGOCIO ---
    # Agrupamos por FECHA (event_date) para tener un reporte diario
    daily_kpis = df.groupBy("event_date").agg(
        # 1. Revenue: Sumar precio SOLO si es compra ('purchase')
        round(sum(when(col("event_type") == 'purchase', col("price")).otherwise(0)), 2).alias("total_revenue"),
        
        # 2. Órdenes: Contar cuántas compras hubo
        count(when(col("event_type") == 'purchase', 1)).alias("total_orders"),
        
        # 3. Visitantes Únicos: Cuántas personas distintas interactuaron (DAU)
        countDistinct("user_id").alias("unique_visitors"),
        
        # 4. Total Tráfico: Cuantos eventos en total
        count("*").alias("total_interactions")
    )
    
    # Calculamos el Ticket Promedio (Revenue / Ordenes)
    # Usamos when para evitar división por cero
    daily_kpis = daily_kpis.withColumn(
        "avg_ticket", 
        when(col("total_orders") > 0, round(col("total_revenue") / col("total_orders"), 2)).otherwise(0)
    )
    
    # Ordenamos por fecha para que se vea bonito
    daily_kpis = daily_kpis.orderBy(desc("event_date"))

    print("--- [GOLD] Previsualización del Reporte ---")
    daily_kpis.show(10)
    
    print(f"--- [GOLD] Escribiendo en {output_path} ---")
    
    # Guardamos en Gold (Sobreescribimos porque es una tabla pequeña de resumen)
    daily_kpis.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)
    
    print("--- [GOLD] Proceso Finalizado ---")
    spark.stop()

if __name__ == "__main__":
    process()