from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Configuraciones comunes para Spark (Para no repetir código)
# Recordamos: Driver en Airflow (512m), Executor en Worker (2g)
spark_conf = {
    "spark.driver.memory": "512m",
    "spark.executor.memory": "2g",
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.port": "7078",
    "spark.blockManager.port": "7079",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}

# Paquetes necesarios (Delta 3.0.0 para Spark 3.5)
spark_packages = 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.0.0'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ecommerce_etl_pipeline',  # Nombre que saldrá en la UI
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'spark', 'delta-lake']
) as dag:

    # 1. LANDING: Descargar de Kaggle a MinIO (Bash)
    ingest_task = BashOperator(
        task_id='ingest_kaggle_to_minio',
        bash_command='python /git/repo/spark/jobs/ingest_kaggle.py'
    )

    # 2. BRONZE: CSV -> Delta Raw (Spark)
    bronze_task = SparkSubmitOperator(
        task_id='process_landing_to_bronze',
        application='/git/repo/spark/jobs/landing_to_bronze.py',
        conn_id='spark_default',
        deploy_mode='client',
        packages=spark_packages,
        conf=spark_conf,
        env_vars={
            'MINIO_ACCESS_KEY': 'admin',
            'MINIO_SECRET_KEY': 'minioadmin',
            'MINIO_ENDPOINT': 'http://minio:9000'
        },
        verbose=True
    )

    # 3. SILVER: Limpieza y Tipado (Spark)
    silver_task = SparkSubmitOperator(
        task_id='process_bronze_to_silver',
        application='/git/repo/spark/jobs/bronze_to_silver.py',
        conn_id='spark_default',
        deploy_mode='client',
        packages=spark_packages,
        conf=spark_conf,
        env_vars={
            'MINIO_ACCESS_KEY': 'admin',
            'MINIO_SECRET_KEY': 'minioadmin',
            'MINIO_ENDPOINT': 'http://minio:9000'
        },
        verbose=True
    )

    # --- DEFINICIÓN DE DEPENDENCIAS ---
    # Esto le dice a Airflow el orden exacto:
    ingest_task >> bronze_task >> silver_task