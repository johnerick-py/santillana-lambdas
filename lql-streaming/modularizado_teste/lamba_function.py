"""
Função Lambda principal para processamento de eventos Kinesis.
"""
import json
import base64
import logging
import time
from datetime import datetime

# Importação dos módulos
from config import KINESIS_NAME, BATCH_SIZE
from utils.s3_utils import save_raw_event, save_processed_payload, save_unknown_pas_event
from utils.redshift_utils import insert_batch_to_redshift
from processors.event_processor import process_event_by_verb

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Buffers para os inserts (chave: nome da tabela, valor: lista de tuplas (processed_record, columns_values))
buffers = {
    "stream.lql": [],
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
            
            logger.info(f"Acumulados registros para a tabela {table}. Tipo: {type(record_data)}. Total no buffer: {len(buffers[table])}")
        except Exception as e:
            logger.error(f"Erro ao acumular registros para a tabela {table}: {e}")

def flush_buffers_to_redshift():
    """
    Insere todos os buffers no Redshift.
    """
    for table, records in buffers.items():
        if records:
            try:
                # Verificar formato dos registros
                for i, record in enumerate(records):
                    if not isinstance(record, tuple) or len(record) != 2:
                        logger.error(f"Formato inválido para registro {i} na tabela {table}: {record}")
                        continue
                
                logger.info(f"Inserindo {len(records)} registros na tabela {table}")
                insert_batch_to_redshift(records, table)
                # Limpa o buffer após inserção bem-sucedida
                buffers[table] = []
            except Exception as e:
                logger.error(f"Falha na inserção para a tabela {table}: {e}")

def process_record(record):
    """
    Processa um único registro do Kinesis.
    """
    record_start = time.time()
    
    try:
        # Decodifica o payload
        decode_start = time.time()
        encoded_data = record["data"]
        payload_str = base64.b64decode(encoded_data).decode('utf-8')
        payload_json = json.loads(payload_str)
        logger.info(f"Decodificação do payload levou {time.time() - decode_start:.2f}s")
        
        # action = payload_json.get("meta", {}).get("action", "")
        # if "updated" in action.lower() or "deleted" in action.lower():
        #     logger.info(f"Ignorando evento com action={action} - não requer processamento")
        #     return True
        
        # Salva o evento bruto no S3 e captura a URI
        s3_start = time.time()
        raw_event_s3_uri = save_raw_event(payload_json, record)
        logger.info(f"Salvamento do evento bruto no S3 levou {time.time() - s3_start:.2f}s")
        
        # Prepara registro para tabela kinesis_log e acumula no buffer
        kinesis_log = build_kinesis_log_record(record, raw_event_s3_uri)
        buffers["infrastructure.kinesis_log"].append((None, kinesis_log))
        
        # Processa o payload conforme o verbID
        process_start = time.time()
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
            
        logger.info(f"Processamento do payload levou {time.time() - process_start:.2f}s")
        
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
    
    # Inicializa o pool de conexões
    # Não precisamos mais disso com a Data API
    
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