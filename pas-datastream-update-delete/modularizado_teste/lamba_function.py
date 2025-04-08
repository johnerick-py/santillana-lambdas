"""
Função Lambda principal para processamento de eventos Kinesis.
Suporta apenas operações UPDATE e DELETE. Os eventos CREATE são tratados por outra Lambda.
"""
import json
import base64
import logging
import time
from datetime import datetime

# Importação dos módulos
from config import KINESIS_NAME, BATCH_SIZE
from utils.s3_utils import save_raw_event, save_processed_payload, save_unknown_pas_event
from utils.redshift_utils import (
    insert_batch_to_redshift, 
    process_upsert_batch,
    process_delete_batch
)
from processors.event_processor import process_event_by_verb

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Buffer para logs do Kinesis (este buffer é mantido pois precisamos registrar todos os eventos)
KINESIS_LOG_BUFFER = {
    "infrastructure.kinesis_log": []
}

# Buffers para os updates
UPDATE_BUFFERS = {
    "gold.dim_school_class": [],
    "gold.dim_school_grade_group": [],
    "gold.dim_school_level_session": [],
    "gold.dim_session": [],
    "gold.dim_section_subject": [],
    "gold.dim_school": [],
    "gold.dim_school_address": [],
    "gold.dim_school_phone": [],
    "gold.dim_school_email": [],
    "gold.dim_user": [],
    "gold.dim_user_phone": [],
    "gold.dim_user_email": [],
    "gold.dim_user_address": [],
    "gold.dim_user_group": [],
    "gold.dim_user_role": [],
    "gold.dim_class_participant": [],
}

# Buffers para os deletes
DELETE_BUFFERS = {
    "gold.dim_school_class": [],
    "gold.dim_school_grade_group": [],
    "gold.dim_school_level_session": [],
    "gold.dim_session": [],
    "gold.dim_section_subject": [],
    "gold.dim_school": [],
    "gold.dim_school_address": [],
    "gold.dim_school_phone": [],
    "gold.dim_school_email": [],
    "gold.dim_user": [],
    "gold.dim_user_phone": [],
    "gold.dim_user_email": [],
    "gold.dim_user_address": [],
    "gold.dim_user_group": [],
    "gold.dim_user_role": [],
    "gold.dim_class_participant": [],
}

NOT_PROCCESS_TABLES = [
    "ARTICLE",
    "CONTENTSCHOOLCLASS",
    "CONTENTSCHOOLLEVELSESSION",
    "CONTACTRELATIONSHIP",
    "SCHOOLIMAGE",
    "USERARTICLENOTIFICATION",
    "USERARTICLEROLE",
    "USERIMAGE"
]

def build_kinesis_log_record(record, s3_uri):
    """
    Monta o registro para a tabela kinesis_log.
    """
    # Extrai o shard_id a partir do eventID
    shard_id = record.get("eventID", "").split(":")[0]
    
    log_record = {
        "kinesis_name": KINESIS_NAME,
        "event_source": record.get("eventSource"),
        "event_version": record.get("eventVersion"),
        "shard_id": shard_id,
        "aws_region": record.get("awsRegion"),
        "partition_key": record.get("partitionKey"),
        "sequence_number": record.get("sequenceNumber"),
        "s3_uri": s3_uri
    }
    return log_record

def accumulate_processed_records(processed_dict, action_type):
    """
    Acumula os registros processados nos buffers por tabela, considerando o tipo de ação.
    Apenas processa ações de tipo 'updated' ou 'deleted'.
    """
    if not processed_dict:
        return
    
    # Seleciona o buffer correto baseado no tipo de ação
    if "updated" in action_type.lower():
        buffers = UPDATE_BUFFERS
    elif "deleted" in action_type.lower():
        buffers = DELETE_BUFFERS
    else:
        logger.warning(f"Tipo de ação ignorado: {action_type}, não será acumulado")
        return
        
    for table, record_data in processed_dict.items():
        try:
            if record_data is not None:
                # Verificar se é uma lista (para tabelas que retornam múltiplos registros)
                if isinstance(record_data, list):
                    for item in record_data:
                        if item is not None:
                            buffers[table].append(item)
                else:
                    # Para tabelas que retornam um único registro
                    buffers[table].append(record_data)
            
            logger.info(f"Acumulados registros para a tabela {table} ({action_type}). Tipo: {type(record_data)}. Total no buffer: {len(buffers[table])}")
        except Exception as e:
            logger.error(f"Erro ao acumular registros para a tabela {table}: {e}")

def flush_buffers_to_redshift():
    """
    Insere todos os buffers no Redshift, tratando diferentes tipos de operações.
    Apenas processa operações de DELETE e UPDATE (UPSERT).
    """
    # Primeiro trata os deletes
    for table, records in DELETE_BUFFERS.items():
        if records:
            try:
                # Verificar formato dos registros
                for i, record in enumerate(records):
                    if not isinstance(record, tuple) or len(record) != 2:
                        logger.error(f"Formato inválido para registro {i} na tabela {table} (DELETE): {record}")
                        continue
                
                logger.info(f"Processando exclusão de {len(records)} registros na tabela {table}")
                process_delete_batch(records, table)
                # Limpa o buffer após processamento
                DELETE_BUFFERS[table] = []
            except Exception as e:
                logger.error(f"Falha na exclusão para a tabela {table}: {e}")
    
    # Depois trata os updates (que podem incluir inserts se necessário)
    for table, records in UPDATE_BUFFERS.items():
        if records:
            try:
                # Verificar formato dos registros
                for i, record in enumerate(records):
                    if not isinstance(record, tuple) or len(record) != 2:
                        logger.error(f"Formato inválido para registro {i} na tabela {table} (UPDATE): {record}")
                        continue
                
                logger.info(f"Processando upsert de {len(records)} registros na tabela {table}")
                process_upsert_batch(records, table)
                # Limpa o buffer após processamento
                UPDATE_BUFFERS[table] = []
            except Exception as e:
                logger.error(f"Falha no upsert para a tabela {table}: {e}")
    
    # Processa os logs do Kinesis
    for table, records in KINESIS_LOG_BUFFER.items():
        if records:
            try:
                logger.info(f"Inserindo {len(records)} registros de log na tabela {table}")
                insert_batch_to_redshift(records, table)
                KINESIS_LOG_BUFFER[table] = []
            except Exception as e:
                logger.error(f"Falha na inserção de logs para a tabela {table}: {e}")

def process_record(record):
    """
    Processa um único registro do Kinesis.
    Trata apenas registros com ações UPDATE e DELETE.
    """
    record_start = time.time()
    
    try:
        # Decodifica o payload
        decode_start = time.time()
        encoded_data = record["data"]
        payload_str = base64.b64decode(encoded_data).decode('utf-8')
        payload_json = json.loads(payload_str)
        logger.info(f"Decodificação do payload levou {time.time() - decode_start:.2f}s")
        
        # Determina o tipo de ação
        action = payload_json.get("meta", {}).get("action", "")
        entityKey = payload_json.get("meta", {}).get("entityKey", "")
        logger.info(f"Verificando evento com action={action}")
        
        # Ignora eventos de criação (created)
        if "created" in action.lower():
            logger.info(f"Ignorando evento com action={action} - será processado por outra Lambda")
            return True
        
        # Verifica se é uma ação suportada (update ou delete)
        if not ("updated" in action.lower() or "deleted" in action.lower()):
            logger.warning(f"Ação não suportada: {action}, ignorando registro")
            return True

        if entityKey is None or entityKey == "":
            logger.warning(f"Evento sem entityKey: {action}, ignorando registro")
            return True
        
        if entityKey in NOT_PROCCESS_TABLES:
            logger.warning(f"Evento com entityKey na blacklist: {entityKey}, ignorando registro")
            return True
            
        # A partir daqui, processa apenas eventos de update e delete
        logger.info(f"Processando evento com action={action}")
        
        # Salva o evento bruto no S3 e captura a URI
        s3_start = time.time()
        raw_event_s3_uri = save_raw_event(payload_json, record)
        logger.info(f"Salvamento do evento bruto no S3 levou {time.time() - s3_start:.2f}s")
        
        # Prepara registro para tabela kinesis_log e acumula no buffer
        kinesis_log = build_kinesis_log_record(record, raw_event_s3_uri)
        KINESIS_LOG_BUFFER["infrastructure.kinesis_log"].append((None, kinesis_log))
        
        # Processa o payload conforme o entityKey
        process_start = time.time()
        processed_dict = process_event_by_verb(payload_json)
        
        if processed_dict is None:
            # EntityKey desconhecido: salva no diretório específico
            save_unknown_pas_event(payload_json)
            logger.warning(f"EntityKey desconhecido no evento com action={action}")
        else:
            # Acumula os registros processados no buffer apropriado
            accumulate_processed_records(processed_dict, action)
            
            # Salva o payload processado no S3
            save_processed_payload(payload_json)
            
            logger.info(f"Processamento do payload {action} levou {time.time() - process_start:.2f}s")
        
        logger.info(f"Registro processado em {time.time() - record_start:.2f}s")
        return True
    except Exception as e:
        logger.error(f"Erro ao processar registro: {e}", exc_info=True)
        return False

def lambda_handler(event, context):
    """
    Função principal da Lambda que processa eventos do Kinesis.
    """
    start_time = time.time()
    logger.info(f"Iniciando processamento de {len(event)} registros")
    
    # Processa cada registro em mini-lotes
    batch_size = BATCH_SIZE  # Usar o valor definido na configuração
    batches_count = (len(event) + batch_size - 1) // batch_size
    
    for batch_num in range(batches_count):
        batch_start = time.time()
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(event))
        
        logger.info(f"Processando lote {batch_num+1}/{batches_count}, registros {start_idx+1}-{end_idx}")
        
        for i in range(start_idx, end_idx):
            process_record(event[i])
        
        # Insere os dados após cada mini-lote
        flush_start = time.time()
        flush_buffers_to_redshift()
        logger.info(f"Lote {batch_num+1} processado e enviado em {time.time() - batch_start:.2f}s (flush: {time.time() - flush_start:.2f}s)")
    
    total_time = time.time() - start_time
    logger.info(f"Processamento concluído com sucesso em {total_time:.2f}s")
    
    return {
        "statusCode": 200,
        "body": f"Processamento concluído com sucesso em {total_time:.2f}s"
    }