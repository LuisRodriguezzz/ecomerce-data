from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, countDistinct, when, round, desc
import os

def get_spark_session():
    return SparkSession.builder \
        .appName("SilverToGold_FullMetrics") \
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
    output_path = "s3a://processed-data/gold/hourly_kpis/"
    
    print(f"--- [GOLD] Leyendo Silver ---")
    df = spark.read.format("delta").load(input_path)
    
    print("--- [GOLD] Calculando Métricas de Embudo y Negocio ---")
    
    # 1. AGREGACIÓN BASE (Contadores)
    # Agrupamos por Fecha + Hora + Categoría (Máxima granularidad útil)
    base_kpis = df.groupBy("event_date", "hour", "main_category").agg(
        
        # A. Dinero
        round(sum(when(col("event_type") == 'purchase', col("price")).otherwise(0)), 2).alias("total_revenue"),
        
        # B. Visitantes Únicos (Tus 'Visitors')
        countDistinct("user_id").alias("unique_visitors"),
        
        # C. Contadores para el Embudo (Funnel)
        count(when(col("event_type") == 'view', 1)).alias("cnt_view"),
        count(when(col("event_type") == 'cart', 1)).alias("cnt_cart"),
        count(when(col("event_type") == 'remove_from_cart', 1)).alias("cnt_remove"),
        count(when(col("event_type") == 'purchase', 1)).alias("cnt_purchase") # Total Orders
    )
    
    # 2. CÁLCULO DE RATIOS (Tus métricas avanzadas)
    gold_final = base_kpis \
        .withColumn("conversion_rate", 
                    when(col("unique_visitors") > 0, 
                         round((col("cnt_purchase") / col("unique_visitors")) * 100, 2)
                    ).otherwise(0)) \
        .withColumn("cart_abandonment_rate", 
                    when(col("cnt_cart") > 0, 
                         round(((col("cnt_cart") - col("cnt_purchase")) / col("cnt_cart")) * 100, 2)
                    ).otherwise(0)) \
        .withColumn("avg_ticket", 
                    when(col("cnt_purchase") > 0, 
                         round(col("total_revenue") / col("cnt_purchase"), 2)
                    ).otherwise(0))

    # Ordenamos
    gold_final = gold_final.orderBy(desc("event_date"), desc("hour"), desc("total_revenue"))

    print("--- [GOLD] Muestra de Datos Finales ---")
    gold_final.show(10)
    
    print(f"--- [GOLD] Escribiendo en {output_path} ---")
    
    gold_final.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .option("overwriteSchema", "true") \
        .save(output_path)
    
    spark.stop()

if __name__ == "__main__":
    process()