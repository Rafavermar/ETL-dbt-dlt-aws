import os
import dlt
import boto3
import pandas as pd
import json
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Cliente de S3 para obtener datos
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

bucket_name = 'dbt-dlt-duckdb-etl'

# Listar todos los archivos CSV en el directorio 'bronze/'
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='bronze/')

if 'Contents' in response:
    for item in response['Contents']:
        key = item['Key']
        if key.endswith('.csv'):  # Procesar solo archivos CSV
            try:
                # Extraer el nombre del archivo para usarlo como dataset_name y como nombre de archivo
                file_name = key.split('/')[-1]  # Conserva la extensión .csv

                # Obtener el objeto de S3
                response = s3_client.get_object(Bucket=bucket_name, Key=key)

                # Leer CSV con delimitador de tabulación
                data = pd.read_csv(response['Body'], delimiter='\t', encoding='utf-8')
                print(f"Data loaded successfully from S3 for {key}.")

                # Configuración del directorio de pipelines y datos
                local_file_path = f'/dbt_project/data/bronze/datacomex/{file_name}'
                os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = local_file_path

                data_pipeline_EL = dlt.pipeline(
                    pipeline_name="load_from_s3",
                    destination="filesystem",
                    dataset_name=file_name.split('.')[0]  # Usar el nombre sin la extensión como dataset_name
                )

                # Convertir DataFrame a lista de diccionarios
                data_list = json.loads(data.to_json(orient='records'))

                # Ejecutar pipeline DLT
                data_pipeline_EL.run(
                    data_list,
                    table_name=file_name.split('.')[0],
                    loader_file_format="csv"  # Asegúrate de que DLT soporte csv como formato de salida
                )
                print(f"Data processed and saved as {local_file_path}")
            except Exception as e:
                print(f"Error processing {key}: {e}")
else:
    print("No files found in the specified bucket/prefix.")
