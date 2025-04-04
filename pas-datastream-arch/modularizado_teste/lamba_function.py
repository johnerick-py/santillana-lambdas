"""
Função Lambda principal para processamento de eventos Kinesis.
"""
import json
import base64
import logging
from datetime import datetime

# Importação dos módulos
from config import KINESIS_NAME
from utils.s3_utils import save_raw_event, save_processed_payload, save_unknown_pas_event
from utils.redshift_utils import init_connection_pool, insert_batch_to_redshift
from processors.event_processor import process_event_by_verb

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Buffers para os inserts (chave: nome da tabela, valor: lista de tuplas (processed_record, columns_values))
buffers = {
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
    "infrastructure.kinesis_log": []
}

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

def accumulate_processed_records(processed_dict):
    """
    Acumula os registros processados nos buffers por tabela.
    """
    if not processed_dict:
        return
        
    for table, record_tuple in processed_dict.items():
        if record_tuple is not None:
            buffers[table].append(record_tuple)

def flush_buffers_to_redshift():
    """
    Insere todos os buffers no Redshift.
    """
    for table, records in buffers.items():
        if records:
            try:
                
                insert_batch_to_redshift(records, table)
                # Limpa o buffer após inserção bem-sucedida
                buffers[table] = []
            except Exception as e:
                logger.error(f"Falha na inserção para a tabela {table}: {e}")
                # Buffers serão limpos na próxima execução

def lambda_handler(event, context):
    """
    Função principal da Lambda que processa eventos do Kinesis.
    """
    logger.info(f"Iniciando processamento de {len(event)} registros")
    
    # Inicializa o pool de conexões
    init_connection_pool()
    
    # Processa cada registro do microbatch
    for record in event:
        try:
            # Decodifica o payload
            encoded_data = record["data"]
            payload_str = base64.b64decode(encoded_data).decode('utf-8')
            payload_json = json.loads(payload_str)
            
            # Salva o evento bruto no S3 e captura a URI
            raw_event_s3_uri = save_raw_event(payload_json, record)
            
            # Prepara registro para tabela kinesis_log e acumula no buffer
            kinesis_log = build_kinesis_log_record(record, raw_event_s3_uri)
            buffers["infrastructure.kinesis_log"].append((None, kinesis_log))
            
            # Processa o payload conforme o verbID
            processed_dict = process_event_by_verb(payload_json)
            
            if processed_dict is None:
                # VerbID desconhecido: salva no diretório específico
                save_unknown_pas_event(payload_json)
            else:
                # Acumula os registros processados em buffers para cada tabela
                accumulate_processed_records(processed_dict)
                logger.info('printando processed dict')
                logger.info(f'{processed_dict}')
                
                # Salva o payload processado no S3
                save_processed_payload(payload_json)
                
        except Exception as e:
            logger.error(f"Erro ao processar registro: {e}", exc_info=True)
    
    # Após processar todos os registros, insere os dados acumulados no Redshift
    flush_buffers_to_redshift()
    
    logger.info("Processamento concluído com sucesso")
    
    return {
        "statusCode": 200,
        "body": "Processamento concluído com sucesso!"
    }