import os
import glob
import kaggle
from minio import Minio
from minio.error import S3Error

# --- CONFIGURACIÓN ---
MINIO_ENDPOINT = "minio:9000"
# Usamos las mismas variables que ya tienes configuradas
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "raw-data"
# Dataset de Olist: Completo, real, pero ligero (50MB)
#DATASET_NAME = "olistbr/brazilian-ecommerce"
DATASET_NAME = "mkechinov/ecommerce-events-history-in-cosmetics-shop"
TEMP_DIR = "/tmp/kaggle_data"  # Directorio temporal en el contenedor

def upload_to_minio_and_clean():
    # 1. Conectar a MinIO
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Asegurar que el bucket existe
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    # 2. Descargar desde Kaggle al directorio temporal
    print(f"--- Descargando dataset: {DATASET_NAME} ---")
    # Authenticate usa las variables de entorno KAGGLE_USERNAME y KAGGLE_KEY automáticamente
    kaggle.api.authenticate()
    
    # Descarga y descomprime automáticamente en /tmp/kaggle_data
    kaggle.api.dataset_download_files(DATASET_NAME, path=TEMP_DIR, unzip=True)
    
    print("--- Descarga y descompresión completada. Iniciando subida a MinIO... ---")

    # 3. Buscar los archivos CSV descomprimidos y subirlos
    # Glob encuentra todos los archivos en la carpeta
    files = glob.glob(f"{TEMP_DIR}/*")
    
    for file_path in files:
        file_name = os.path.basename(file_path)
        # Opcional: Filtrar solo CSVs si hay basura
        if not file_name.endswith('.csv'):
            continue

        object_name = f"landing/{file_name}"
        print(f"-> Subiendo {file_name} a {BUCKET_NAME}/{object_name}...")
        
        try:
            # fput_object sube un archivo desde el sistema de archivos local a MinIO
            client.fput_object(
                BUCKET_NAME,
                object_name,
                file_path,
            )
            print(f"   [OK] Subido correctamente.")
            
            # 4. BORRAR el archivo local inmediatamente para liberar espacio
            os.remove(file_path)
            print(f"   [Clean] Archivo local eliminado.")
            
        except S3Error as err:
            print(f"   [ERROR] Falló la subida: {err}")

    # Limpiar directorio temporal
    try:
        os.rmdir(TEMP_DIR)
    except:
        pass # Si queda algo, no importa tanto, se borrará al reiniciar el contenedor
    
    print("--- Proceso Finalizado ---")

if __name__ == "__main__":
    upload_to_minio_and_clean()