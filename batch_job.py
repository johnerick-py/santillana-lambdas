import psycopg2
import os
import boto3
from urllib.parse import urlparse
import time
import datetime
import concurrent.futures
import json
import logging
import uuid

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações do Redshift
REDSHIFT_CONN = {
    'dbname': 'datalake',
    'user': 'admin',
    'password': 'IVDMBvybjo842++',
    'host': 'pas-prod-redshift-workgroup.888577054267.us-east-1.redshift-serverless.amazonaws.com',
    'port': '5439'
}

# Configurações gerais
S3_BUCKET = 'pas-prod-silver'
TEMP_DIR = '/tmp/etl_batches'
CHECKPOINT_PREFIX = 'checkpoints/lrs_etl/'
BATCH_SIZE = 10000  # Número de registros por batch
MAX_WORKERS = 5     # Número de workers para processamento paralelo

# Variáveis de controle
last_connection_time = time.time()
connection = None
RECONNECT_INTERVAL = 30 * 60  # 30 minutos em segundos

# Definição de tabelas e consultas
TABLE_QUERIES = {
    "dim_lrs_actor": """
        INSERT INTO gold.dim_lrs_actor (
            statement_id, lrs_id, client_id, actor_name, actor_mbox, actor_type, dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.statement.actor.name::VARCHAR AS actor_name,
            event.statement.actor.mbox::VARCHAR AS actor_mbox,
            event.statement.actor.objecttype::VARCHAR AS actor_type,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
    """,
    
    "dim_lrs_object": """
        INSERT INTO gold.dim_lrs_object (
            statement_id, lrs_id, client_id, object_id, object_type, interaction_type, 
            definition_description_es, dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.statement.object.id::VARCHAR AS object_id,
            event.statement.object.objectType::VARCHAR AS object_type,
            event.statement.object.definition.interactiontype::VARCHAR AS interaction_type,
            event.statement.object.definition.description.es::VARCHAR AS definition_description_es,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
    """,
    
    "dim_lrs_object_http": """
        INSERT INTO gold.dim_lrs_object_http (
            statement_id, lrs_id, client_id, activity_id, resource_ref_id, resource_id,
            source_id, learning_unit_id, section_subject_id, school_class_id,
            learning_objective_id, type_intelligence, page_ref, student_id,
            role_id, level_code, learning_level_id, adventure_code, activity_type,
            act_type, unit_id, learning_objective_id_api, subject_grade_id, school_id,
            dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.statement.object.definition.extensions."http://acttype".activityrefid::VARCHAR AS activity_id,
            event.statement.object.definition.extensions."http://acttype".resourceid::VARCHAR AS resource_ref_id,
            event.statement.object.definition.extensions."http://acttype".resourcerefid::VARCHAR AS resource_id,
            event.statement.object.definition.extensions."http://acttype".sourcerefid::VARCHAR AS source_id,
            event.statement.object.definition.extensions."http://acttype".learningunitrefid::VARCHAR AS learning_unit_id,
            event.statement.object.definition.extensions."http://acttype".sectionsubjectrefid::VARCHAR AS section_subject_id,
            event.statement.object.definition.extensions."http://acttype".schoolclassid::VARCHAR AS school_class_id,
            event.statement.object.definition.extensions."http://acttype".learningobjectiverefid::VARCHAR AS learning_objective_id,
            event.statement.object.definition.extensions."http://acttype".typeactivityintelligence::VARCHAR AS type_intelligence,
            event.statement.object.definition.extensions."http://acttype".pagerefid::VARCHAR AS page_ref,
            event.statement.object.definition.extensions."http://acttype".studentrefid::VARCHAR AS student_id,
            event.statement.object.definition.extensions."http://acttype".rolid::VARCHAR AS role_id,
            event.statement.object.definition.extensions."http://acttype".levelcode::VARCHAR AS level_code,
            event.statement.object.definition.extensions."http://acttype".learninglevelrefid::VARCHAR AS learning_level_id,
            event.statement.object.definition.extensions."http://acttype".adventurecode::VARCHAR AS adventure_code,
            event.statement.object.definition.extensions."http://acttype".typeactivityvalue::VARCHAR AS activity_type,
            event.statement.object.definition.extensions."http://acttype".acttype::VARCHAR AS act_type,
            event.statement.object.definition.extensions."http://acttype".unitid::VARCHAR AS unit_id,
            event.statement.object.definition.extensions."http://acttype".learningobjectiverefidapi::VARCHAR AS learning_objective_id_api,
            event.statement.object.definition.extensions."http://acttype".subjectgraderefid::VARCHAR AS subject_grade_id,
            event.statement.object.definition.extensions."http://acttype".schoolrefid::VARCHAR AS school_id,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
            AND event.statement.object.definition.extensions."http://acttype" is not null
    """,
    
    "dim_lrs_result": """
        INSERT INTO gold.dim_lrs_result (
            statement_id, lrs_id, client_id, score_raw, score_max, score_min,
            response, success_status, dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.statement.result.score."raw"::VARCHAR AS score_raw,
            event.statement.result.score."max"::VARCHAR AS score_max,
            event.statement.result.score."min"::VARCHAR AS score_min,
            event.statement.result.response::VARCHAR AS response,
            event.statement.result.success::BOOL AS success_status,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
    """,
    
    "dim_lrs_statement": """
        INSERT INTO gold.dim_lrs_statement (
            statement_id, active_status, lrs_id, client_id, related_activities,
            date_stored, dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.active::BOOL AS active_status,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.relatedactivities[0]::VARCHAR AS related_activities,
            event.statement.stored::VARCHAR AS date_stored,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
    """,
    
    "dim_lrs_verb": """
        INSERT INTO gold.dim_lrs_verb (
            statement_id, lrs_id, client_id, verb_id, display_und, display_en_us,
            display_es_es, display_de_de, display_es, display_en_gb, display_fr_fr,
            dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.statement.verb.id::VARCHAR AS verb_id,
            event.statement.verb.display.und::VARCHAR AS display_und,
            event.statement.verb.display.enus::VARCHAR AS display_en_us,
            event.statement.verb.display.eses::VARCHAR AS display_es_es,
            event.statement.verb.display.dede::VARCHAR AS display_de_de,
            event.statement.verb.display.es::VARCHAR AS display_es,
            event.statement.verb.display.engb::VARCHAR AS display_en_gb,
            event.statement.verb.display.frfr::VARCHAR AS display_fr_fr,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
    """,
    
    "dim_lrs_authority": """
        INSERT INTO gold.dim_lrs_authority (
            statement_id, lrs_id, client_id, authority_name, authority_mbox, dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.statement.authority.name::VARCHAR AS authority_name,
            event.statement.authority.mbox::VARCHAR AS authority_mbox,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
    """,
    
    "dim_lrs_context": """
        INSERT INTO gold.dim_lrs_context (
            statement_id, lrs_id, client_id, context_parent, context_parent_object_type, dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.statement.context.contextactivities.parent[0].id::VARCHAR AS context_parent,
            event.statement.context.contextactivities.parent[0].objecttype::VARCHAR AS context_parent_object_type,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
    """,
    
    "dim_lrs_result_scorm": """
        INSERT INTO gold.dim_lrs_result_scorm (
            statement_id, lrs_id, client_id, multiple_recording, correct_responses_pattern, response, dt_load
        )
        SELECT
            event.id::VARCHAR AS statement_id,
            event.lrsid::VARCHAR AS lrs_id,
            event.clientid::VARCHAR AS client_id,
            event.statement.result.extensions."http://scorm&46;com/extensions/usadata".multiplerecording::VARCHAR AS multiple_recording,
            event.statement.result.extensions."http://scorm&46;com/extensions/usadata".correctresponsespattern::VARCHAR AS correct_responses_pattern,
            event.statement.result.extensions."http://scorm&46;com/extensions/usadata".response::VARCHAR AS response,
            dt_load
        FROM
            stream_raw.lrs_events_all
        WHERE
            dt_load > %s
            AND dt_load <= %s
    """
}

# Inicializa cliente S3
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_connection():
    """Estabelece uma conexão com o Redshift."""
    global last_connection_time
    
    try:
        conn = psycopg2.connect(**REDSHIFT_CONN)
        conn.autocommit = False
        
        logger.info(f"[{datetime.datetime.now()}] Conectado ao Redshift com sucesso")
        last_connection_time = time.time()
        
        return conn
    except Exception as e:
        logger.error(f"[{datetime.datetime.now()}] Erro ao conectar ao Redshift: {str(e)}")
        raise

def check_connection_refresh():
    """Verifica se é necessário renovar a conexão com o Redshift."""
    global connection, last_connection_time
    
    current_time = time.time()
    if current_time - last_connection_time >= RECONNECT_INTERVAL:
        logger.info(f"[{datetime.datetime.now()}] Renovando conexão com o Redshift após {RECONNECT_INTERVAL/60} minutos...")
        
        try:
            if connection and not connection.closed:
                connection.commit()  # Commit de quaisquer transações pendentes
                connection.close()
                logger.info(f"[{datetime.datetime.now()}] Conexão anterior fechada com sucesso")
        except Exception as e:
            logger.error(f"[{datetime.datetime.now()}] Erro ao fechar conexão anterior: {str(e)}")
        
        connection = get_connection()
        return connection
    
    return connection

def get_latest_timestamp(conn, table_name):
    """Obtém o último timestamp processado para determinada tabela."""
    try:
        cursor = conn.cursor()
        # Verifica se a tabela existe
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = 'gold' 
                AND table_name = '{table_name}'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.warning(f"[{datetime.datetime.now()}] A tabela gold.{table_name} não existe.")
            cursor.close()
            return datetime.datetime(2000, 1, 1)
        
        # Tenta obter o último timestamp
        cursor.execute(f"SELECT MAX(dt_load) FROM gold.{table_name}")
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0]:
            return result[0]
        else:
            # Se não houver registros, retorna uma data bem antiga
            return datetime.datetime(2000, 1, 1)
    except Exception as e:
        logger.error(f"[{datetime.datetime.now()}] Erro ao obter último timestamp para {table_name}: {str(e)}")
        # Em caso de erro, também retorna uma data antiga
        return datetime.datetime(2000, 1, 1)

def get_data_range(conn, table_name):
    """Obtém o intervalo de dados a serem processados."""
    # Obtém o último timestamp processado
    last_processed = get_latest_timestamp(conn, table_name)
    
    # Define o fim do intervalo como a data/hora atual
    end_timestamp = datetime.datetime.now()
    
    # Converte para string no formato esperado pelo Redshift
    last_processed_str = last_processed.strftime('%Y-%m-%d %H:%M:%S')
    end_timestamp_str = end_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"[{datetime.datetime.now()}] Intervalo para {table_name}: {last_processed_str} até {end_timestamp_str}")
    
    return last_processed_str, end_timestamp_str

def save_checkpoint(table_name, timestamp):
    """Salva o checkpoint do processamento em S3."""
    try:
        checkpoint_data = {
            'table': table_name,
            'last_processed': timestamp,
            'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Cria diretório temporário se não existir
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Gera um nome para o arquivo temporário
        temp_file_path = os.path.join(TEMP_DIR, f'{table_name}_checkpoint.json')
        
        # Escreve o checkpoint em um arquivo local temporário
        with open(temp_file_path, 'w') as f:
            json.dump(checkpoint_data, f)
        
        # Faz upload para o S3
        s3_key = f"{CHECKPOINT_PREFIX}{table_name}_checkpoint.json"
        s3_client.upload_file(temp_file_path, S3_BUCKET, s3_key)
        
        logger.info(f"[{datetime.datetime.now()}] Checkpoint salvo para {table_name}: {timestamp}")
        
        # Remove o arquivo temporário
        os.remove(temp_file_path)
        
    except Exception as e:
        logger.error(f"[{datetime.datetime.now()}] Erro ao salvar checkpoint para {table_name}: {str(e)}")

def load_data_batch(table_name, query, start_timestamp, end_timestamp):
    """Carrega um batch de dados para a tabela gold."""
    try:
        # Obtém uma nova conexão para o batch
        conn = get_connection()
        cursor = conn.cursor()
        
        logger.info(f"[{datetime.datetime.now()}] Executando batch para {table_name} de {start_timestamp} a {end_timestamp}")
        
        # Executa a consulta com os parâmetros de data
        cursor.execute(query, (start_timestamp, end_timestamp))
        
        # Obtém o número de linhas afetadas
        row_count = cursor.rowcount
        
        # Commit das alterações
        conn.commit()
        
        logger.info(f"[{datetime.datetime.now()}] Batch para {table_name} concluído: {row_count} registros processados")
        
        # Fecha cursor e conexão
        cursor.close()
        conn.close()
        
        return {
            'table': table_name,
            'rows_processed': row_count,
            'start': start_timestamp,
            'end': end_timestamp
        }
        
    except Exception as e:
        logger.error(f"[{datetime.datetime.now()}] Erro ao processar batch para {table_name}: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise

def process_table(table_name, query):
    """Processa uma tabela completa em batches."""
    try:
        logger.info(f"[{datetime.datetime.now()}] Iniciando processamento da tabela {table_name}")
        
        # Estabelece uma conexão para a tabela
        conn = get_connection()
        
        # Obtém o intervalo de dados a serem processados
        start_timestamp, end_timestamp = get_data_range(conn, table_name)
        
        # Fecha a conexão, pois cada batch terá sua própria conexão
        conn.close()
        
        # Inicia o processamento em batch
        logger.info(f"[{datetime.datetime.now()}] Processando tabela {table_name} em batches...")
        
        # Executa o carregamento em batch único
        result = load_data_batch(table_name, query, start_timestamp, end_timestamp)
        
        # Salva o checkpoint após o processamento
        save_checkpoint(table_name, end_timestamp)
        
        return result
        
    except Exception as e:
        logger.error(f"[{datetime.datetime.now()}] Erro ao processar tabela {table_name}: {str(e)}")
        if 'conn' in locals() and conn and not conn.closed:
            conn.close()
        raise

def process_all_tables():
    """Processa todas as tabelas definidas."""
    results = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submete o processamento de cada tabela
        future_to_table = {
            executor.submit(process_table, table_name, query): table_name 
            for table_name, query in TABLE_QUERIES.items()
        }
        
        # Processa os resultados à medida que são concluídos
        for future in concurrent.futures.as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                result = future.result()
                results[table_name] = result
            except Exception as e:
                results[table_name] = {
                    'table': table_name,
                    'error': str(e),
                    'status': 'failed'
                }
    
    return results

def write_etl_summary(results):
    """Escreve um resumo da execução do ETL no S3."""
    try:
        # Prepara os dados do relatório
        report_data = {
            'execution_id': str(uuid.uuid4()),
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tables_processed': len(results),
            'results': results
        }
        
        # Calcula estatísticas
        total_rows = sum(r.get('rows_processed', 0) for r in results.values() if isinstance(r, dict) and 'rows_processed' in r)
        report_data['total_rows_processed'] = total_rows
        
        # Cria diretório temporário se não existir
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Gera um nome para o arquivo temporário
        timestamp = int(time.time())
        temp_file_name = f"etl_summary_{timestamp}.json"
        temp_file_path = os.path.join(TEMP_DIR, temp_file_name)
        
        # Escreve o relatório em um arquivo local temporário
        with open(temp_file_path, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        # Faz upload para o S3
        s3_key = f"etl_reports/lrs_etl/summary_{timestamp}.json"
        s3_client.upload_file(temp_file_path, S3_BUCKET, s3_key)
        
        logger.info(f"[{datetime.datetime.now()}] Resumo do ETL salvo em s3://{S3_BUCKET}/{s3_key}")
        
        # Remove o arquivo temporário
        os.remove(temp_file_path)
        
        return f"s3://{S3_BUCKET}/{s3_key}"
        
    except Exception as e:
        logger.error(f"[{datetime.datetime.now()}] Erro ao salvar resumo do ETL: {str(e)}")
        raise

def main():
    """Função principal que executa o job."""
    global connection
    
    # Marca o tempo de início
    start_time = time.time()
    
    # Cria diretório temporário se não existir
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    try:
        # Conecta ao Redshift (conexão principal)
        connection = get_connection()
        
        logger.info(f"[{datetime.datetime.now()}] Iniciando processo de ETL em batch...")
        
        # Processa todas as tabelas
        results = process_all_tables()
        
        # Escreve o resumo do ETL
        summary_path = write_etl_summary(results)
        
        # Calcula estatísticas
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"[{datetime.datetime.now()}] Processamento concluído!")
        logger.info(f"[{datetime.datetime.now()}] Total de tabelas processadas: {len(results)}")
        logger.info(f"[{datetime.datetime.now()}] Resumo salvo em: {summary_path}")
        logger.info(f"[{datetime.datetime.now()}] Tempo total de execução: {duration/60:.2f} minutos")
        
        return {
            "status": "success",
            "tables_processed": len(results),
            "summary_file": summary_path,
            "execution_time": duration
        }
        
    except Exception as e:
        logger.error(f"[{datetime.datetime.now()}] Erro geral: {str(e)}")
        raise
        
    finally:
        # Fecha a conexão principal se estiver aberta
        if connection and not connection.closed:
            connection.close()
            logger.info(f"[{datetime.datetime.now()}] Conexão principal com Redshift fechada")

if __name__ == "__main__":
    main()