"""
Utilitários para interação com o banco de dados Redshift.
"""
import time
import logging
import psycopg2
from psycopg2 import pool

from config import REDSHIFT_CONFIG, MAX_RETRIES
from utils.s3_utils import save_error_batch

logger = logging.getLogger()

# Pool de conexões (inicializado como None)
connection_pool = None

def init_connection_pool():
    """
    Inicializa o pool de conexões com o Redshift.
    """
    global connection_pool
    if connection_pool is None:
        try:
            connection_pool = psycopg2.pool.SimpleConnectionPool(5, 20, **REDSHIFT_CONFIG)
            logger.info("Pool de conexões inicializado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao inicializar pool de conexões: {e}")
            raise
    return connection_pool

def build_processed_record(input_data, db_columns, exclude_columns=['dt_load', 'dt_end']):
    """
    Monta o processed_record e dicionário de colunas-valores para query dinâmica.
    """
    if exclude_columns is None:
        exclude_columns = []

    # Converte a lista de entrada em um dicionário
    input_dict = dict(input_data)

    # Monta o processed_record na ordem das colunas
    processed_record = tuple(
        input_dict.get(column, None) if column not in exclude_columns else None
        for column in db_columns
    )

    # Cria o dicionário de colunas e valores para a query
    columns_values_dict = {
        column: input_dict[column]
        for column in db_columns
        if column in input_dict and column not in exclude_columns
    }

    return processed_record, columns_values_dict

def insert_batch_to_redshift(records_with_columns, table_name, max_retries=MAX_RETRIES):
    """
    Insere um lote de registros no Redshift com colunas dinâmicas.
    """
    retries = 0
    while retries < max_retries:
        conn = None
        try:
            init_connection_pool()
            conn = connection_pool.getconn()
            
            if conn.closed != 0:
                logger.info("Conexão fechada. Obtendo nova conexão.")
                conn = connection_pool.getconn()
                
            cursor = conn.cursor()
            
            for processed_record, columns_values in records_with_columns:
                if not columns_values:  # Skip if no columns to insert
                    continue
                    
                columns = ', '.join(columns_values.keys())
                placeholders = ', '.join(['%s'] * len(columns_values))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
                
                cursor.execute(query, list(columns_values.values()))
                
            conn.commit()
            logger.info(f"Inseridos {len(records_with_columns)} registros na tabela {table_name}.")
            return True
            
        except Exception as e:
            if "SSL connection has been closed unexpectedly" in str(e):
                logger.warning("Conexão SSL fechada inesperadamente. Tentando reestabelecer.")
                if conn:
                    conn.close()
                retries += 1
                time.sleep(1)
            else:
                logger.error(f"Erro ao inserir no Redshift: {e}")
                # Salva o batch com erro no S3
                save_error_batch(records_with_columns, table_name)
                return False
                
        finally:
            if conn:
                try:
                    connection_pool.putconn(conn)
                except Exception as e:
                    logger.error(f"Erro ao devolver conexão para o pool: {e}")
    
    logger.error("Máximo de tentativas atingido. Dados não foram inseridos.")
    # Salva o batch com erro no S3
    save_error_batch(records_with_columns, table_name)
    return False