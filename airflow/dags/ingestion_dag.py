from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ecommerce_ingestion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'ingestion']
) as dag:

    # Tarea: Ejecutar el script de ingestión en el clúster de Spark
    ingest_task = SparkSubmitOperator(
        task_id='ingest_raw_data',
        application='/opt/repo_code/spark/jobs/ingest_raw.py', # Ruta dentro del contenedor
        conn_id='spark_default', # Definiremos esto abajo
        # Estos paquetes son OBLIGATORIOS para conectar Spark con S3/MinIO
        packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        env_vars={
            'MINIO_ACCESS_KEY': 'admin', # O tus credenciales reales
            'MINIO_SECRET_KEY': 'minioadmin'
        },
        verbose=True
    )

    ingest_task