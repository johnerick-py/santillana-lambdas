#!/usr/bin/env python
# coding: utf-8

"""
Funções para escrita e persistência de dados.
"""

import time
import psycopg2
from pyspark.sql.functions import col, hash
from utils.aws_utils import get_aws_secret
from utils.schema_utils import get_result_scorm_schema, get_result_schema
from config.settings import (
    BATCH_SIZE, 
    COALESCE_PARTITIONS, 
    get_temp_path, 
    get_s3_output_path,
    REDSHIFT_HOST, 
    REDSHIFT_PORT, 
    REDSHIFT_DB, 
    REDSHIFT_USER, 
    REDSHIFT_PASSWORD,
    REDSHIFT_IAM_ROLE,
    RESULT_TABLE_NAME,
    RESULT_SCORM_TABLE_NAME
)


def save_dataframe_in_batches(df, schema_columns, table_name, batch_size=BATCH_SIZE, coalesce_partitions=COALESCE_PARTITIONS):
    """
    Salva um DataFrame em lotes para evitar problemas de memória.
    
    Args:
        df: DataFrame Spark a ser salvo.
        schema_columns (list): Lista de colunas para organizar o DataFrame.
        table_name (str): Nome da tabela.
        batch_size (int): Número de lotes.
        coalesce_partitions (int): Número de partições para coalesce.
        
    Returns:
        tuple: (temp_path, s3_path) Caminhos onde os dados foram salvos.
    """
    print(f"Salvando {table_name} em {batch_size} lotes...")
    
    # Organiza as colunas na ordem esperada
    df_ordered = df.select(*schema_columns)
    
    # Define o caminho de saída
    temp_path = get_temp_path(table_name)
    s3_path = get_s3_output_path(table_name)
    
    # Medir tempo total de escrita
    write_start = time.time()
    
    # Processa e salva em lotes
    total_records = 0
    for i in range(batch_size):
        print(f"Processando batch {i+1}/{batch_size}...")
        df_batch = df_ordered.filter((hash(col("statement_id")) % batch_size) == i)
        
        batch_count = df_batch.count()
        total_records += batch_count
        
        if batch_count > 0:
            batch_path = f"{s3_path}/part_{i}/"
            print(f"Salvando batch {i+1} com {batch_count:,} registros em {batch_path}")
            
            # Salva diretamente no S3 com menor número de partições
            df_batch.coalesce(coalesce_partitions).write.mode("overwrite").parquet(batch_path)
        else:
            print(f"Batch {i+1} vazio. Pulando...")
    
    write_time = time.time() - write_start
    print(f"Total de {total_records:,} registros salvos em {write_time:.2f} segundos")
    
    return temp_path, s3_path


def register_table_in_catalog(spark, table_name, location_path):
    """
    Registra a tabela no catálogo do Hive.
    
    Args:
        spark: Sessão Spark.
        table_name (str): Nome da tabela.
        location_path (str): Caminho dos dados.
        
    Returns:
        bool: True se registrado com sucesso, False caso contrário.
    """
    try:
        print(f"Registrando tabela {table_name} no catálogo...")
        
        # Cria o database se não existir
        spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        
        # Remove a tabela se já existir
        spark.sql(f"DROP TABLE IF EXISTS gold.{table_name}")
        
        # Cria a tabela apontando para o local dos dados
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold.{table_name}
            USING PARQUET
            LOCATION '{location_path}'
        """)
        
        print(f"Tabela gold.{table_name} registrada com sucesso")
        return True
    except Exception as e:
        print(f"Erro ao registrar tabela {table_name}: {e}")
        return False


def get_redshift_connection():
    """
    Obtém conexão com o Redshift, primeiro tentando variáveis de ambiente, 
    depois AWS Secrets Manager.
    
    Returns:
        Connection: Conexão psycopg2 com o Redshift.
        
    Raises:
        Exception: Se não conseguir obter conexão.
    """
    try:
        # Tenta primeiro com variáveis de ambiente
        if REDSHIFT_HOST and REDSHIFT_USER and REDSHIFT_PASSWORD:
            conn = psycopg2.connect(
                dbname=REDSHIFT_DB,
                host=REDSHIFT_HOST,
                port=REDSHIFT_PORT,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD
            )
            return conn
        
        # Se não tiver variáveis de ambiente, tenta AWS Secrets Manager
        print("Obtendo credenciais Redshift do AWS Secrets Manager...")
        conn_database = get_aws_secret()
        conn = psycopg2.connect(
            dbname=conn_database['dbname'],
            host=conn_database['host'],
            port=5439,
            user=conn_database['username'],
            password=conn_database['password']
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao Redshift: {e}")
        raise


def copy_to_redshift(table_name, s3_path, iam_role=REDSHIFT_IAM_ROLE):
    """
    Copia os dados de uma tabela para o Redshift via COPY.
    
    Args:
        table_name (str): Nome da tabela.
        s3_path (str): Caminho S3 dos dados.
        iam_role (str): Role IAM para acesso ao S3.
        
    Returns:
        bool: True se copiado com sucesso, False caso contrário.
    """
    print(f"Copiando {table_name} para o Redshift...")
    try:
        copy_start = time.time()
        conn = get_redshift_connection()
        cur = conn.cursor()

        copy_command = f"""
        COPY gold.{table_name}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
        """
        
        cur.execute(copy_command)
        conn.commit()
        cur.close()
        conn.close()
        
        copy_time = time.time() - copy_start
        print(f"Tempo de COPY no Redshift: {copy_time:.2f} segundos")
        return True
    except Exception as e:
        print(f"Erro ao copiar para o Redshift: {e}")
        return False


def save_and_register_datasets(spark, df_transformed_result_scorm, df_transformed_result, copy_to_redshift=False):
    """
    Salva e registra todos os datasets transformados.
    
    Args:
        spark: Sessão Spark.
        df_transformed_result_scorm: DataFrame de resultado SCORM transformado.
        df_transformed_result: DataFrame de resultado transformado.
        copy_to_redshift (bool): Se deve copiar para o Redshift.
        
    Returns:
        bool: True se todas as operações foram bem-sucedidas, False caso contrário.
    """
    success = True
    
    # Obter esquemas
    _, expected_schema_result_scorm = get_result_scorm_schema()
    _, expected_schema_result = get_result_schema()
    
    # Salvar result_scorm
    print("\nSalvando result_scorm...")
    scorm_ordered_columns = list(expected_schema_result_scorm.keys())
    _, scorm_s3_path = save_dataframe_in_batches(
        df_transformed_result_scorm,
        scorm_ordered_columns,
        RESULT_SCORM_TABLE_NAME
    )
    
    # Registrar result_scorm no catálogo
    if not register_table_in_catalog(spark, RESULT_SCORM_TABLE_NAME, scorm_s3_path):
        success = False
    
    # Salvar result
    print("\nSalvando result...")
    result_ordered_columns = list(expected_schema_result.keys())
    _, result_s3_path = save_dataframe_in_batches(
        df_transformed_result,
        result_ordered_columns,
        RESULT_TABLE_NAME
    )
    
    # Registrar result no catálogo
    if not register_table_in_catalog(spark, RESULT_TABLE_NAME, result_s3_path):
        success = False
    
    # Copiar para o Redshift (opcional)
    if copy_to_redshift and success:
        print("\n=== COPIANDO PARA O REDSHIFT ===")
        if not copy_to_redshift(RESULT_SCORM_TABLE_NAME, scorm_s3_path):
            success = False
        
        if not copy_to_redshift(RESULT_TABLE_NAME, result_s3_path):
            success = False
    
    return success