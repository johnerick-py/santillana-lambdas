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

# Mapeamento de chaves primárias das tabelas
TABLE_PRIMARY_KEYS = {
    "gold.dim_school_class": "school_class_id",
    "gold.dim_school_grade_group": "school_grade_group_id",
    "gold.dim_school_level_session": "school_level_session_id",
    "gold.dim_session": "session_id",
    "gold.dim_section_subject": "school_id",
    "gold.dim_school": "school_id",
    "gold.dim_school_address": "school_address_id",
    "gold.dim_school_phone": "school_phone_id",
    "gold.dim_school_email": "school_email_id",
    "gold.dim_user": "user_id",
    "gold.dim_user_phone": "user_phone_id",
    "gold.dim_user_address": "user_address_id",
    "gold.dim_user_email": "user_email_id",
    "gold.dim_user_group": "group_id",
    "gold.dim_user_role": "user_role_id",
    "gold.dim_class_participant": "user_id"
}

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
            # Adicionar timeout para obter conexão
            conn = connection_pool.getconn()
            
            if conn.closed != 0:
                logger.info("Conexão fechada. Obtendo nova conexão.")
                conn = connection_pool.getconn()
                
            # Configurar timeout para operações no banco
            conn.set_session(autocommit=False)
            cursor = conn.cursor()
            
            # Adicionar log para monitoramento
            start_time = time.time()
            logger.info(f"Iniciando inserção de {len(records_with_columns)} registros na tabela {table_name}")
            
            for processed_record, columns_values in records_with_columns:
                if not columns_values:  # Skip if no columns to insert
                    continue
                    
                columns = ', '.join(columns_values.keys())
                placeholders = ', '.join(['%s'] * len(columns_values))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
                
                # Adicionar timeout para a execução da query
                cursor.execute(query, list(columns_values.values()))
                
            conn.commit()
            elapsed_time = time.time() - start_time
            logger.info(f"Inseridos {len(records_with_columns)} registros na tabela {table_name}. Tempo: {elapsed_time:.2f}s")
            return True
            
        except psycopg2.OperationalError as e:
            elapsed_time = time.time() - start_time if 'start_time' in locals() else 0
            logger.warning(f"Erro operacional ao inserir no Redshift após {elapsed_time:.2f}s: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            retries += 1
            # Backoff exponencial para retries
            sleep_time = min(2 ** retries, 10)
            logger.info(f"Tentativa {retries}/{max_retries}. Aguardando {sleep_time}s antes de tentar novamente.")
            time.sleep(sleep_time)
        
        except Exception as e:
            logger.error(f"Erro ao inserir no Redshift: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
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

def check_record_exists(conn, table_name, primary_key_value, primary_key_field=None):
    """
    Verifica se um registro existe no Redshift baseado em sua chave primária.
    
    Args:
        conn: Conexão com o banco de dados
        table_name: Nome da tabela a ser consultada
        primary_key_value: Valor da chave primária
        primary_key_field: Nome do campo da chave primária (opcional, usa o mapeamento padrão se não fornecido)
    
    Returns:
        bool: True se o registro existir, False caso contrário
    """
    if not primary_key_field:
        primary_key_field = TABLE_PRIMARY_KEYS.get(table_name)
        if not primary_key_field:
            logger.error(f"Chave primária não mapeada para a tabela {table_name}")
            return False
    
    if primary_key_value is None:
        logger.warning(f"Valor de chave primária nulo para tabela {table_name}")
        return False
    
    try:
        cursor = conn.cursor()
        query = f"SELECT 1 FROM {table_name} WHERE {primary_key_field} = %s LIMIT 1"
        cursor.execute(query, (primary_key_value,))
        result = cursor.fetchone()
        exists = result is not None
        logger.info(f"Verificação de existência para {table_name}.{primary_key_field}={primary_key_value}: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Erro ao verificar existência do registro: {e}")
        return False

def update_record_in_redshift(conn, table_name, columns_values, primary_key_field=None, primary_key_value=None):
    """
    Atualiza um registro no Redshift.
    
    Args:
        conn: Conexão com o banco de dados
        table_name: Nome da tabela a ser atualizada
        columns_values: Dicionário com as colunas e valores a serem atualizados
        primary_key_field: Nome do campo da chave primária (opcional)
        primary_key_value: Valor da chave primária (opcional, usa columns_values se não fornecido)
    
    Returns:
        bool: True se a atualização for bem-sucedida, False caso contrário
    """
    if not primary_key_field:
        primary_key_field = TABLE_PRIMARY_KEYS.get(table_name)
        if not primary_key_field:
            logger.error(f"Chave primária não mapeada para a tabela {table_name}")
            return False
    
    if not primary_key_value and primary_key_field in columns_values:
        primary_key_value = columns_values[primary_key_field]
    
    if not primary_key_value:
        logger.error(f"Valor de chave primária não fornecido para update na tabela {table_name}")
        return False
    
    # Remove a chave primária do dicionário de valores a serem atualizados
    update_values = {k: v for k, v in columns_values.items() if k != primary_key_field}
    
    if not update_values:
        logger.warning(f"Nenhum valor para atualizar na tabela {table_name}")
        return True  # Nada para atualizar é considerado sucesso
    
    try:
        cursor = conn.cursor()
        set_clause = ", ".join([f"{key} = %s" for key in update_values.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key_field} = %s"
        
        # Valores para a cláusula SET, seguidos pelo valor da chave primária
        values = list(update_values.values()) + [primary_key_value]
        
        logger.info(f"Executando update: {query} com valores {values}")
        cursor.execute(query, values)
        affected_rows = cursor.rowcount
        logger.info(f"Atualização realizada: {affected_rows} linhas afetadas")
        return True
    except Exception as e:
        logger.error(f"Erro ao atualizar registro: {e}")
        return False

def delete_record_from_redshift(conn, table_name, primary_key_value, primary_key_field=None):
    """
    Exclui um registro do Redshift.
    
    Args:
        conn: Conexão com o banco de dados
        table_name: Nome da tabela
        primary_key_value: Valor da chave primária
        primary_key_field: Nome do campo da chave primária (opcional)
    
    Returns:
        bool: True se a exclusão for bem-sucedida, False caso contrário
    """
    if not primary_key_field:
        primary_key_field = TABLE_PRIMARY_KEYS.get(table_name)
        if not primary_key_field:
            logger.error(f"Chave primária não mapeada para a tabela {table_name}")
            return False
    
    if primary_key_value is None:
        logger.error(f"Valor de chave primária nulo para exclusão na tabela {table_name}")
        return False
    
    try:
        cursor = conn.cursor()
        query = f"DELETE FROM {table_name} WHERE {primary_key_field} = %s"
        logger.info(f"Executando delete: {query} com valor {primary_key_value}")
        cursor.execute(query, (primary_key_value,))
        affected_rows = cursor.rowcount
        logger.info(f"Exclusão realizada: {affected_rows} linhas afetadas")
        return True
    except Exception as e:
        logger.error(f"Erro ao excluir registro: {e}")
        return False

def process_upsert_batch(records_with_columns, table_name, max_retries=MAX_RETRIES):
    """
    Processa um lote de registros no Redshift com verificação de existência (update ou insert).
    
    Args:
        records_with_columns: Lista de tuplas (processed_record, columns_values)
        table_name: Nome da tabela
        max_retries: Número máximo de tentativas
    
    Returns:
        bool: True se o processamento for bem-sucedido, False caso contrário
    """
    retries = 0
    primary_key_field = TABLE_PRIMARY_KEYS.get(table_name)
    
    if not primary_key_field:
        logger.error(f"Chave primária não mapeada para a tabela {table_name}")
        return False
    
    while retries < max_retries:
        conn = None
        try:
            init_connection_pool()
            conn = connection_pool.getconn()
            
            if conn.closed != 0:
                logger.info("Conexão fechada. Obtendo nova conexão.")
                conn = connection_pool.getconn()
            
            conn.set_session(autocommit=False)
            
            start_time = time.time()
            logger.info(f"Iniciando upsert de {len(records_with_columns)} registros na tabela {table_name}")
            
            insert_count = 0
            update_count = 0
            
            for processed_record, columns_values in records_with_columns:
                if not columns_values:
                    continue
                
                primary_key_value = columns_values.get(primary_key_field)
                
                if primary_key_value is None:
                    logger.warning(f"Registro sem chave primária para tabela {table_name}, ignorando")
                    continue
                
                # Verifica se o registro existe
                if check_record_exists(conn, table_name, primary_key_value, primary_key_field):
                    # Atualiza o registro existente
                    success = update_record_in_redshift(conn, table_name, columns_values, primary_key_field)
                    if success:
                        update_count += 1
                else:
                    # Insere um novo registro
                    columns = ', '.join(columns_values.keys())
                    placeholders = ', '.join(['%s'] * len(columns_values))
                    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    
                    cursor = conn.cursor()
                    cursor.execute(query, list(columns_values.values()))
                    insert_count += 1
            
            conn.commit()
            elapsed_time = time.time() - start_time
            logger.info(f"Upsert concluído na tabela {table_name}: {insert_count} inserções, {update_count} atualizações. Tempo: {elapsed_time:.2f}s")
            return True
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            
            logger.error(f"Erro ao processar upsert: {e}")
            retries += 1
            
            if retries >= max_retries:
                logger.error(f"Máximo de tentativas atingido para upsert na tabela {table_name}")
                save_error_batch(records_with_columns, f"upsert_{table_name}")
                return False
            
            sleep_time = min(2 ** retries, 10)
            logger.info(f"Tentativa {retries}/{max_retries}. Aguardando {sleep_time}s antes de tentar novamente.")
            time.sleep(sleep_time)
            
        finally:
            if conn:
                try:
                    connection_pool.putconn(conn)
                except Exception as e:
                    logger.error(f"Erro ao devolver conexão para o pool: {e}")
    
    return False

def process_delete_batch(records_with_columns, table_name, max_retries=MAX_RETRIES):
    """
    Processa um lote de exclusões no Redshift.
    
    Args:
        records_with_columns: Lista de tuplas (processed_record, columns_values)
        table_name: Nome da tabela
        max_retries: Número máximo de tentativas
    
    Returns:
        bool: True se o processamento for bem-sucedido, False caso contrário
    """
    retries = 0
    primary_key_field = TABLE_PRIMARY_KEYS.get(table_name)
    
    if not primary_key_field:
        logger.error(f"Chave primária não mapeada para a tabela {table_name}")
        return False
    
    while retries < max_retries:
        conn = None
        try:
            init_connection_pool()
            conn = connection_pool.getconn()
            
            if conn.closed != 0:
                logger.info("Conexão fechada. Obtendo nova conexão.")
                conn = connection_pool.getconn()
            
            conn.set_session(autocommit=False)
            
            start_time = time.time()
            logger.info(f"Iniciando exclusão de {len(records_with_columns)} registros na tabela {table_name}")
            
            delete_count = 0
            skipped_count = 0
            
            for processed_record, columns_values in records_with_columns:
                if not columns_values:
                    skipped_count += 1
                    continue
                
                primary_key_value = columns_values.get(primary_key_field)
                
                if primary_key_value is None:
                    logger.warning(f"Registro sem chave primária para tabela {table_name}, ignorando")
                    skipped_count += 1
                    continue
                
                # Verifica se o registro existe antes de tentar excluir
                if check_record_exists(conn, table_name, primary_key_value, primary_key_field):
                    success = delete_record_from_redshift(conn, table_name, primary_key_value, primary_key_field)
                    if success:
                        delete_count += 1
                else:
                    logger.info(f"Registro com {primary_key_field}={primary_key_value} não encontrado para exclusão")
                    skipped_count += 1
            
            conn.commit()
            elapsed_time = time.time() - start_time
            logger.info(f"Exclusão concluída na tabela {table_name}: {delete_count} registros excluídos, {skipped_count} ignorados. Tempo: {elapsed_time:.2f}s")
            return True
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            
            logger.error(f"Erro ao processar exclusão: {e}")
            retries += 1
            
            if retries >= max_retries:
                logger.error(f"Máximo de tentativas atingido para exclusão na tabela {table_name}")
                save_error_batch(records_with_columns, f"delete_{table_name}")
                return False
            
            sleep_time = min(2 ** retries, 10)
            logger.info(f"Tentativa {retries}/{max_retries}. Aguardando {sleep_time}s antes de tentar novamente.")
            time.sleep(sleep_time)
            
        finally:
            if conn:
                try:
                    connection_pool.putconn(conn)
                except Exception as e:
                    logger.error(f"Erro ao devolver conexão para o pool: {e}")
    
    return False