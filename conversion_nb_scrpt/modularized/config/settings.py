#!/usr/bin/env python
# coding: utf-8

"""
Configurações globais do projeto.
"""

import os
from dotenv import load_dotenv

# Carregar configurações do arquivo .env
load_dotenv()

# Configurações AWS
AWS_PROFILE = os.getenv("AWS_PROFILE", "john-prod")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Caminhos S3 - IMPORTANTE: Use s3a:// em vez de s3://
S3_TEMP_PATH = os.getenv("S3_TEMP_PATH", "s3a://pas-prod-silver/wicco-temp").replace("s3://", "s3a://")
S3_OUTPUT_PATH = os.getenv("S3_OUTPUT_PATH", "s3a://pas-prod-gold").replace("s3://", "s3a://")
S3_INPUT_BASE_PATH = os.getenv("S3_INPUT_BASE_PATH", "s3a://pas-prod-silver/wicco-historical-final").replace("s3://", "s3a://")

# Caminhos locais
LOCAL_TEMP_DIR = os.getenv("LOCAL_TEMP_DIR", "/tmp/spark-wicco-temp")

# Credenciais Redshift
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "")
REDSHIFT_USER = os.getenv("REDSHIFT_USER", "")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "")
REDSHIFT_IAM_ROLE = os.getenv("REDSHIFT_IAM_ROLE", "")

# Secrets Manager
SECRET_NAME = os.getenv("SECRET_NAME", "prod/redshift/aplication/access")

# Configurações de processamento
PROCESS_896_DATA = os.getenv("PROCESS_896_DATA", "false").lower() == "true"
COPY_TO_REDSHIFT = os.getenv("COPY_TO_REDSHIFT", "true").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
COALESCE_PARTITIONS = int(os.getenv("COALESCE_PARTITIONS", "1"))

# Definições de anos e IDs a processar por padrão
YEARS_DEFAULT = ["2024", "2025"]
#ID_PREFIXES_DEFAULT = ["7447", "7449", "7450", "7460"]
ID_PREFIXES_DEFAULT = ["7449"]
if PROCESS_896_DATA:
    ID_PREFIXES_DEFAULT.append("896")
    
# Colunas para truncar
TRUNCATE_COLUMNS = ["id_system", "statement_id", "lrs_id", "client_id"]

# Valores de statement_id a filtrar
EXCLUDED_STATEMENT_IDS = [
    '749b3af7-a667-4a73-9a53-adb605619df0',
    'e04ef4c3-8368-4d9e-8804-5048fcdbac72'
]

# Configurações das tabelas
RESULT_TABLE_NAME = "dim_lrs_result"
RESULT_SCORM_TABLE_NAME = "dim_lrs_result_scorm"

# Caminhos para os arquivos
def get_input_path(id_prefix, year, data_type="results"):
    """Obtém o caminho completo do S3 para os dados de entrada"""
    path = f"{S3_INPUT_BASE_PATH}/{id_prefix}/{year}/{data_type}/"
    return path

def get_temp_path(table_name):
    """Obtém o caminho temporário para os dados de uma tabela"""
    return f"{LOCAL_TEMP_DIR}/{table_name}_batch"

def get_s3_output_path(table_name):
    """Obtém o caminho S3 de saída para uma tabela"""
    return f"{S3_OUTPUT_PATH}/gold/{table_name}"