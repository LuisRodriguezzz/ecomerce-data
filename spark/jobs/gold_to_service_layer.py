from pyspark.sql import SparkSession
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
    
    # 1. Leer la Fuente de Verdad (GOLD en MinIO)
    input_path = "s3a://processed-data/gold/hourly_kpis/"
    print(f"--- [PUBLISH] Leyendo Gold desde {input_path} ---")
    
    df_gold = spark.read.format("delta").load(input_path)
    
    # 2. Configurar Destino (Postgres)
    # En una empresa real, aquí irían las credenciales de Snowflake/Redshift
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow" 
    table_name = "public.gold_hourly_kpis_report"
    
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    
    print(f"--- [PUBLISH] Escribiendo en Postgres ({table_name}) ---")
    
    # Mode Overwrite: En reportes pequeños, solemos reemplazar la tabla completa.
    # En Big Data real, usaríamos 'append' con cuidado de no duplicar.
    df_gold.write \
        .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
        
    print("--- [PUBLISH] Carga Exitosa. Power BI ya puede leer los datos. ---")
    spark.stop()

if __name__ == "__main__":
    process()