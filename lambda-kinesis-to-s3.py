import json
import base64
import boto3
import os
import re
from datetime import datetime, timezone

def transform_keys(obj):
    """
    Função recursiva para transformar todas as chaves em um objeto JSON:
    - Remove caracteres '_' e '-' das chaves
    - Converte todas as chaves para lowercase
    - Remove aspas simples de valores em 'relatedagents'
    """
    if isinstance(obj, dict):
        new_dict = {}
        for key, value in obj.items():
            # Transformar a chave: remover '_', '-' e converter para lowercase
            new_key = key.replace('_', '').replace('-', '').lower()
            
            # Tratamento especial para o campo relatedAgents/relatedagents
            if new_key == 'relatedagents' and isinstance(value, list):
                new_value = []
                for item in value:
                    if isinstance(item, str):
                        # Remover aspas simples, aspas duplas e aspas escapadas
                        cleaned_item = item.replace("'", "").replace('\\"', '').replace('"', '')
                        new_value.append(cleaned_item)
                    else:
                        new_value.append(transform_keys(item))
                new_dict[new_key] = new_value
            else:
                # Transformar o valor recursivamente se for dict ou list
                new_dict[new_key] = transform_keys(value)
        return new_dict
    elif isinstance(obj, list):
        # Se for uma lista, aplicar a transformação para cada item da lista
        return [transform_keys(item) for item in obj]
    else:
        # Se for um valor primitivo, retornar sem alteração
        return obj

def save_to_s3(json_data):
    """
    Salva o JSON processado no bucket S3
    """
    # Inicializar o cliente S3
    s3_client = boto3.client('s3')
    
    # Bucket de destino fixo
    bucket_name = "record-age-exceeded-lrs-baa"
    
    # Criar nome do arquivo no formato event_{year}_{month}_{day}_{timestamp}.json
    now_utc = datetime.now(timezone.utc)
    year = now_utc.strftime('%Y')
    month = now_utc.strftime('%m')
    day = now_utc.strftime('%d')
    timestamp = now_utc.strftime('%H%M%S%f')
    
    file_name = f"event_{year}_{month}_{day}_{timestamp}.json"
    
    try:
        # Converter JSON para string
        json_string = json.dumps(json_data)
        
        # Fazer upload do arquivo para o S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_string,
            ContentType='application/json'
        )
        
        print(f"Arquivo {file_name} salvo com sucesso no bucket {bucket_name}")
        return file_name
        
    except Exception as e:
        print(f"Erro ao salvar no S3: {str(e)}")
        raise e

def lambda_handler(event, context):
    """
    Função Lambda para processar registros do Kinesis e salvá-los no S3
    no bucket record-age-exceeded-lrs-baa
    """
    print("Evento recebido do Kinesis:", json.dumps(event))
    
    # Verificar se existem registros no evento
    if 'Records' not in event or not event['Records']:
        print("Nenhum registro encontrado no evento")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Nenhum registro para processar'})
        }
    
    # Lista para armazenar os nomes dos arquivos salvos
    saved_files = []
    
    # Processar cada registro do Kinesis
    for i, record in enumerate(event['Records']):
        print(f"\n--- Processando registro {i+1} ---")
        
        try:
            # Obter o dado codificado em base64
            encoded_data = record['kinesis']['data']
            
            # Decodificar o payload do Kinesis
            decoded_data = base64.b64decode(encoded_data).decode('utf-8')
            print(f"Dados originais decodificados")
            
            # PRIMEIRO: Converter para JSON
            json_data = json.loads(decoded_data)
            
            # Transformar as chaves do JSON
            transformed_json = transform_keys(json_data)
            
            # Salvar no S3
            file_name = save_to_s3(transformed_json)
            saved_files.append(file_name)
            
        except Exception as e:
            print(f"Erro ao processar registro {i+1}: {str(e)}")
            print(f"Exception detalhes: {type(e).__name__}: {str(e)}")
            # Imprimir o traceback para debugging
            import traceback
            traceback.print_exc()
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processados {len(event["Records"])} registros com sucesso',
            'saved_files': saved_files
        })
    }