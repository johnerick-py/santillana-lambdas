import boto3
import pyarrow.parquet as pq
import io

# Nome do perfil configurado no AWS SSO
session = boto3.Session(profile_name='john-prod')

# Configurações do S3
bucket_name = 'pas-prod-silver'
prefix = 'historical_v3_01/guatemala/companies/sessions_all_users/'  # Caminho onde estão os arquivos Parquet
#s3://pas-prod-silver/historical_v3_latest/argentina/companies/sessions_all_users/

# Inicializar o cliente do S3 usando o perfil do SSO
s3_client = session.client('s3')

# Listar arquivos Parquet no bucket
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Filtrar apenas arquivos Parquet
parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

# Função para ler o esquema de um arquivo Parquet
def get_parquet_schema(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    parquet_file = io.BytesIO(response['Body'].read())
    table = pq.read_table(parquet_file)
    return table.schema

# Verificar o esquema do primeiro arquivo Parquet encontrado
if parquet_files:
    print(f"Analisando o arquivo: {parquet_files[0]}")
    schema = get_parquet_schema(bucket_name, parquet_files[0])
    columns = [field.name for field in schema]
    print("Colunas encontradas:")
    for col in columns:
        print(f"- {col}")
else:
    print("Nenhum arquivo Parquet encontrado no bucket.")


