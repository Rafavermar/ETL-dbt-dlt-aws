import os
import boto3
from dotenv import load_dotenv
import subprocess
import schedule
import time

# Cargar las variables de entorno
load_dotenv()

# Configurar cliente de S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)


def list_s3_files(bucket, prefix):
    """ Lista los nombres de archivo CSV en un prefijo específico de un bucket de S3. """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return {item['Key'].split('/')[-1] for item in response.get('Contents', []) if item['Key'].endswith('.csv')}


def list_local_files(directory):
    """ Lista los nombres de archivo CSV en un directorio local y sus subdirectorios. """
    csv_files = set()
    if os.path.exists(directory):
        for root, dirs, files in os.walk(directory):
            csv_files.update(file for file in files if file.endswith('.csv'))
    return csv_files


def run_dlt_script(script_path):
    """ Ejecuta un script de Python. """
    subprocess.run(['python', script_path], check=True)


def run_dbt_commands():
    """ Ejecuta comandos dbt dentro del contenedor Docker. """
    subprocess.run(['docker', 'exec', 'dbt_demo', '/bin/bash', '-c', 'dbt run'], check=True)
    subprocess.run(['docker', 'exec', 'dbt_demo', '/bin/bash', '-c', 'dbt test'], check=True)


def run_streamlit():
    """ Ejecuta la aplicación Streamlit. """
    subprocess.run(['docker', 'exec', 'dbt_demo', '/bin/bash', '-c',
                    'streamlit run streamlit/dashboard.py --server.port 8080'], check=True)


def fetch_and_compare_files(s3_bucket, s3_prefix, local_directory, script_path):
    """ Compara los archivos de S3 con los archivos locales y ejecuta un script DLT si es necesario. """
    s3_files = list_s3_files(s3_bucket, s3_prefix)
    local_files = list_local_files(local_directory)

    if s3_files == local_files:
        print(f"Todos los archivos en {s3_prefix} están presentes localmente.")
    else:
        print(f"Archivos faltantes en {local_directory}. Ejecutando script DLT...")
        run_dlt_script(script_path)


def main():
    bucket_name = 'dbt-dlt-duckdb-etl'
    data_prefix = 'bronze/'
    metadata_prefix = 'metadata/'
    local_data_dir = 'C:/Users/jrver/PycharmProjects/Python/ETL-dbt-dlt-aws/dbt_project/data/bronze/datacomex/comex_taric'
    local_metadata_dir = 'C:/Users/jrver/PycharmProjects/Python/ETL-dbt-dlt-aws/dbt_project/data/bronze/datacomex/metadata'
    data_script_path = 'C:/Users/jrver/PycharmProjects/Python/ETL-dbt-dlt-aws/dlt_scripts/fetch_comexdata_dlt.py'
    metadata_script_path = 'C:/Users/jrver/PycharmProjects/Python/ETL-dbt-dlt-aws/dlt_scripts/fetch_metadata_dlt.py'

    # Realizar comparaciones y ejecutar DLT si es necesario
    fetch_and_compare_files(bucket_name, data_prefix, local_data_dir, data_script_path)
    fetch_and_compare_files(bucket_name, metadata_prefix, local_metadata_dir, metadata_script_path)

    # Ejecutar comandos DBT y Streamlit solo si todos los archivos están presentes
    run_dbt_commands()
    run_streamlit()


def job():
    print("Ejecutando la tarea programada...")
    main()


# Programar la ejecución
schedule.every().month.at("00:00").do(job)  # Ajusta la hora según necesites

while True:
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
    main()
