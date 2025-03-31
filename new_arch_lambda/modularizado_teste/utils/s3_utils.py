"""
Utilitários para interação com o Amazon S3.
"""
import json
import boto3
import uuid
import logging
from datetime import datetime

from config import (
    BUCKET_NAME, 
    KEY_PREFIX_RAW, 
    KEY_PREFIX_EVENT_LOG, 
    KEY_PREFIX_UNKNOWN, 
    KEY_PREFIX_PROCESSED
)

logger = logging.getLogger()
s3_client = boto3.client('s3')

def upload_json_to_s3(json_data, key_prefix, filename=None):
    """
    Faz upload de um objeto JSON para o S3.
    """
    now = datetime.utcnow()
    if filename is None:
        # Gera um nome único: timestamp + UUID
        key = f"{key_prefix}{now.strftime('%Y/%m/%d/%H%M%S')}-{uuid.uuid4()}.json"
    else:
        key = f"{key_prefix}{filename}"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(json_data, ensure_ascii=False),
        ContentType='application/json'
    )
    
    s3_uri = f"s3://{BUCKET_NAME}/{key}"
    logger.info(f"Arquivo salvo no S3: {s3_uri}")
    return s3_uri

def save_raw_event(payload_json, record):
    """
    Salva o evento cru (payload original) no S3 e retorna a URI.
    """
    now = datetime.utcnow()
    # Monta o caminho conforme mes/dia/hora atual
    key_prefix = f"{KEY_PREFIX_EVENT_LOG}{now.strftime('%m')}/{now.strftime('%d')}/{now.strftime('%H')}/"
    # Nome do arquivo: partition_key_sequence_number.json
    filename = f"{record['partitionKey']}_{record['sequenceNumber']}.json"
    s3_uri = upload_json_to_s3(payload_json, key_prefix, filename)
    return s3_uri

def save_processed_payload(payload):
    """
    Salva um payload processado no S3 conforme o path final.
    """
    now = datetime.utcnow()
    # Path: s3://.../verb_id/id_lrsid_uuid.json
    lrs_id = payload.get("lrs_id", "unknown")
    verb_id = payload.get("verbID", "unknown").split("/")[-1]
    filename = f"{lrs_id}_{verb_id}_{uuid.uuid4()}.json"
    key = f"{KEY_PREFIX_PROCESSED}{verb_id}/{now.strftime('%m')}/{now.strftime('%d')}/{now.strftime('%H')}/{lrs_id}/{filename}"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False),
        ContentType='application/json'
    )
    
    logger.info(f"Payload processado salvo em: s3://{BUCKET_NAME}/{key}")
    return f"s3://{BUCKET_NAME}/{key}"

def save_unknown_verb_event(payload):
    """
    Salva um evento com verbID desconhecido no S3.
    """
    verb_id = payload.get("verbID", "unknown").split("/")[-1]
    filename = f"{verb_id}_{uuid.uuid4()}.json"
    key = f"{KEY_PREFIX_UNKNOWN}{filename}"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False),
        ContentType='application/json'
    )
    
    logger.info(f"Evento com verbID desconhecido salvo em: s3://{BUCKET_NAME}/{key}")
    return f"s3://{BUCKET_NAME}/{key}"

def save_error_batch(records, table_name):
    """
    Salva um lote que falhou na inserção no S3.
    """
    from config import KEY_PREFIX_ERROR
    
    error_filename = f"{table_name.replace('.', '_')}_{uuid.uuid4()}.json"
    error_path = f"{KEY_PREFIX_ERROR}{error_filename}"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=error_path,
        Body=json.dumps([rec[1] for rec in records], ensure_ascii=False),
        ContentType='application/json'
    )
    
    logger.error(f"Batch com erro salvo em S3: s3://{BUCKET_NAME}/{error_path}")