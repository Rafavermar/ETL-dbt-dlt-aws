import os
import dlt
import boto3
import pandas as pd
import json
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Define the environment variable for DLT to find
os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = 'C:/Users/jrver/PycharmProjects/Python/ETL-dbt-dlt-aws/data/'

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
                file_name = key.split('/')[-1]
                year_month = file_name.split('_')[-1].split('.')[0]

                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                data = pd.read_csv(response['Body'], delimiter='\t', encoding='utf-16')
                print(f"Data loaded successfully from S3 for {key}.")

                local_dir = os.getenv('LOCAL_DIR_PATH') + f"/comex_taric/{year_month}/"
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                local_file_path = os.path.join(local_dir, file_name)

                # Save the DataFrame to CSV with UTF-8 encoding
                data.to_csv(local_file_path, index=False, encoding='utf-8')
                print(f"Data processed and saved in UTF-8 encoding at {local_file_path}")
            except Exception as e:
                print(f"Error processing {key}: {e}")
else:
    print("No files found in the specified bucket/prefix.")
