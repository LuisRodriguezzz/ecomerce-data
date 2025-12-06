from airflow import DAG
from airflow.operators.bash import BashOperator # <--- Usamos este en vez de PythonOperator
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

    ingest_task