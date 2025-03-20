import json
import base64
import boto3
import os
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
    print("Evento recebido - iniciando processamento")
    
    # Lista para armazenar os nomes dos arquivos salvos
    saved_files = []
    
    # Verificar se o evento é uma string ou já está em formato JSON
    if isinstance(event, str):
        try:
            # Remover possíveis caracteres extra no início ou fim
            event_str = event.strip()
            
            # Verificar se a string parece ser um array JSON
            if event_str.startswith('[') and event_str.endswith(']'):
                print("Detectado array JSON como string")
                # Tentar analisar como JSON array
                events = json.loads(event_str)
                
                # Processar cada evento no array
                for kinesis_event in events:
                    if 'eventSource' in kinesis_event and kinesis_event['eventSource'] == 'aws:kinesis' and 'data' in kinesis_event:
                        try:
                            # Decodificar dados Base64
                            base64_data = kinesis_event['data']
                            decoded_data = base64.b64decode(base64_data).decode('utf-8')
                            json_data = json.loads(decoded_data)
                            
                            # Transformar as chaves
                            transformed_json = transform_keys(json_data)
                            
                            # Salvar no S3
                            file_name = save_to_s3(transformed_json)
                            saved_files.append(file_name)
                            print(f"Processou evento Kinesis do array: {kinesis_event.get('eventID', 'unknown ID')}")
                        except Exception as e:
                            print(f"Erro ao processar evento do array: {str(e)}")
            # Se não for um array, tentar outros métodos
            else:
                print("Evento não é um array JSON, tentando outras abordagens")
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON do evento: {str(e)}")
    
    # Se o evento já for um objeto Python (lista ou dicionário)
    elif isinstance(event, list):
        print(f"Evento recebido como lista Python com {len(event)} itens")
        # Processar cada evento na lista
        for kinesis_event in event:
            if isinstance(kinesis_event, dict) and 'eventSource' in kinesis_event and kinesis_event['eventSource'] == 'aws:kinesis' and 'data' in kinesis_event:
                try:
                    # Decodificar dados Base64
                    base64_data = kinesis_event['data']
                    decoded_data = base64.b64decode(base64_data).decode('utf-8')
                    json_data = json.loads(decoded_data)
                    
                    # Transformar as chaves
                    transformed_json = transform_keys(json_data)
                    
                    # Salvar no S3
                    file_name = save_to_s3(transformed_json)
                    saved_files.append(file_name)
                    print(f"Processou evento Kinesis da lista: {kinesis_event.get('eventID', 'unknown ID')}")
                except Exception as e:
                    print(f"Erro ao processar evento da lista: {str(e)}")
    
    # Se for um único evento do Kinesis
    elif isinstance(event, dict):
        # Verificar se o evento é um único registro do Kinesis
        if 'eventSource' in event and event['eventSource'] == 'aws:kinesis' and 'data' in event:
            try:
                # Decodificar dados Base64
                decoded_data = base64.b64decode(event['data']).decode('utf-8')
                json_data = json.loads(decoded_data)
                
                # Transformar as chaves do JSON
                transformed_json = transform_keys(json_data)
                
                # Salvar no S3
                file_name = save_to_s3(transformed_json)
                saved_files.append(file_name)
            except Exception as e:
                print(f"Erro ao processar evento único: {str(e)}")
        
        # Verificar se o evento contém uma lista de registros
        elif 'Records' in event and isinstance(event['Records'], list):
            for i, record in enumerate(event['Records']):
                print(f"\n--- Processando registro {i+1} ---")
                
                try:
                    # Verificar se é um registro do Kinesis
                    if 'kinesis' in record and 'data' in record['kinesis']:
                        # Obter o dado codificado em base64
                        encoded_data = record['kinesis']['data']
                        
                        # Decodificar o payload do Kinesis
                        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
                        print(f"Dados decodificados com sucesso")
                        
                        # Converter para JSON
                        json_data = json.loads(decoded_data)
                        
                        # Transformar as chaves do JSON
                        transformed_json = transform_keys(json_data)
                        
                        # Salvar no S3
                        file_name = save_to_s3(transformed_json)
                        saved_files.append(file_name)
                    else:
                        print(f"Registro {i+1} não contém dados do Kinesis esperados")
                        
                except Exception as e:
                    print(f"Erro ao processar registro {i+1}: {str(e)}")
                    print(f"Exception detalhes: {type(e).__name__}: {str(e)}")
                    # Imprimir o traceback para debugging
                    import traceback
                    traceback.print_exc()
    
    # Se mesmo assim não processou nada, último método: procurar diretamente por strings 'data' contendo Base64
    if not saved_files:
        try:
            # Converter para string se não for
            event_str = event if isinstance(event, str) else json.dumps(event)
            
            # Procurar diretamente por campos 'data' que parecem Base64
            import re
            # Padrão para encontrar campos data com conteúdo Base64
            data_matches = re.findall(r'"data"\s*:\s*"([A-Za-z0-9+/=]+)"', event_str)
            
            if data_matches:
                print(f"Encontrou {len(data_matches)} campos 'data' que parecem Base64")
                for i, base64_data in enumerate(data_matches):
                    try:
                        # Decodificar dados Base64
                        decoded_data = base64.b64decode(base64_data).decode('utf-8')
                        json_data = json.loads(decoded_data)
                        
                        # Transformar as chaves
                        transformed_json = transform_keys(json_data)
                        
                        # Salvar no S3
                        file_name = save_to_s3(transformed_json)
                        saved_files.append(file_name)
                        print(f"Processou campo 'data' Base64 #{i+1} direto da string")
                    except Exception as e:
                        print(f"Erro ao processar campo 'data' Base64 #{i+1}: {str(e)}")
        except Exception as e:
            print(f"Erro ao procurar campos 'data' Base64 diretamente: {str(e)}")
            
    # Verificar se processamos algum evento
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processados {len(saved_files)} registros com sucesso',
            'saved_files': saved_files
        })
    }