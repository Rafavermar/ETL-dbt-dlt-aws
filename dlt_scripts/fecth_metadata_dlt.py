import os
import boto3
import pandas as pd
import json
from dotenv import load_dotenv
import dlt

# Cargar variables de entorno desde .env
load_dotenv()

# Configurar la ruta local para almacenar los datos descargados
os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = 'C:/Users/jrver/PycharmProjects/Python/ETL-dbt-dlt-aws/dbt_project/data/dlt_pipeline_info'

# Cliente de S3 para obtener datos
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

bucket_name = 'dbt-dlt-duckdb-etl'
metadata_prefix = 'metadata/'

# Listar todos los archivos CSV en el directorio 'metadata/'
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)

if 'Contents' in response:
    for item in response['Contents']:
        key = item['Key']
        if key.endswith('.csv'):  # Procesar solo archivos CSV
            try:
                file_name = key.split('/')[-1]

                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                data = pd.read_csv(response['Body'], delimiter='\t', encoding='utf-8')
                print(f"Data loaded successfully from S3 for {key}.")

                local_dir = os.getenv('LOCAL_DIR_PATH') + "/metadata/"
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                local_file_path = os.path.join(local_dir, file_name)

                # Convertir DataFrame a lista de diccionarios
                data_list = json.loads(data.to_json(orient='records'))

                # Crear y ejecutar la pipeline de DLT
                data_pipeline = dlt.pipeline(
                    pipeline_name="load_metadata_from_s3",
                    destination="filesystem",
                    dataset_name=file_name.split('.')[0]
                )
                data_pipeline.run(
                    data=data_list,
                    table_name=file_name.split('.')[0],
                    loader_file_format="jsonl"
                )

                data.to_csv(local_file_path, index=False, encoding='utf-8')
                print(f"Data saved in UTF-8 encoding at {local_file_path}")
            except Exception as e:
                print(f"Error processing {key}: {e}")
else:
    print("No files found in the specified bucket/prefix.")
