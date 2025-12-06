from airflow import DAG
from airflow.operators.bash import BashOperator # <--- Usamos este en vez de PythonOperator
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
    tags=['kaggle', 'ingestion', 'minio']
) as dag:

    # Tarea: Ejecutar el script Python directamente usando la ruta
    # Es equivalente a abrir la terminal y escribir "python /ruta/archivo.py"
    ingest_task = BashOperator(
        task_id='ingest_kaggle_to_minio',
        # Aquí pasas la ruta directa, igual que en SparkSubmitOperator
        bash_command='python /git/repo/spark/jobs/ingest_kaggle.py'
    )
    

    to_bronze = SparkSubmitOperator(
        task_id='process_landing_to_bronze',
        application='/git/repo/spark/jobs/landing_to_bronze.py',
        conn_id='spark_default',
        deploy_mode='client',   
        packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.0.0',
        # -------------------
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.driver.memory": "512m",
            "spark.executor.memory": "2g",
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.port": "7078",
            "spark.blockManager.port": "7079"
             },
        env_vars={
            'MINIO_ACCESS_KEY': 'admin',
            'MINIO_SECRET_KEY': 'minioadmin'
        },
        verbose=True
    )

    ingest_task >> to_bronze