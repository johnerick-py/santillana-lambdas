import json
import boto3
import base64
import time
from datetime import datetime
import os

# Inicializar clientes
sqs = boto3.client('sqs')
kinesis = boto3.client('kinesis')
s3 = boto3.client('s3')

# Configurações
DLQ_URL = 'https://sqs.us-east-1.amazonaws.com/888577054267/dead-queue-lrs-baa'  # Atualize com a URL correta da sua DLQ
MAX_MESSAGES = 10  # Número de mensagens a processar de uma vez
WAIT_TIME = 20  # Tempo de espera para mensagens (em segundos)

# Configuração do S3
S3_BUCKET = 'pas-prod-landing-zone'
S3_PREFIX = 'lrs-baa-stream'

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

def get_event_type(verb_id):
    """
    Extrai o tipo de evento do verbID para uso na organização de arquivos
    """
    # Extrair a última parte do verbID para usar como tipo de evento
    if verb_id and '/' in verb_id:
        event_type = verb_id.split('/')[-1]
    else:
        # Usar um valor padrão se não conseguir extrair
        event_type = "unknown"
    
    return event_type

def insert_to_s3(json_data, event_type):
    """
    Insere o JSON processado no S3, separado por ano/mês com nome do arquivo incluindo o dia
    """
    try:
        # Obter data atual para a estrutura de pastas e nome do arquivo
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        timestamp = now.strftime("%H%M%S%f")
        
        # Criar chave S3 com estrutura: {prefix}/{year}/{month}/{event_type}_{day}_{timestamp}.json
        s3_key = f"{S3_PREFIX}/{year}/{month}/{event_type}_{day}_{timestamp}.json"
        
        # Converter JSON para string
        json_string = json.dumps(json_data)
        
        # Fazer upload para o S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_string,
            ContentType='application/json'
        )
        
        # Obter o timestamp atual
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Imprimir mensagem de sucesso com timestamp
        print(f"SUCCESS: [{current_time}] Registro inserido no S3: s3://{S3_BUCKET}/{s3_key}")
        return True
        
    except Exception as e:
        print(f"ERROR: Falha na inserção no S3: {str(e)}")
        return False

def process_record(record_data):
    """
    Processa um registro do Kinesis e insere no S3
    """
    try:
        # Verificar se o dado está em bytes e decodificar se necessário
        if isinstance(record_data, bytes):
            decoded_data = record_data.decode('utf-8')
        else:
            decoded_data = record_data
        
        # Converter para JSON
        json_data = json.loads(decoded_data)
        
        # Log para diagnóstico dos dados originais (limitado para evitar logs muito grandes)
        print(f"DEBUG: Estrutura JSON original: {json.dumps(json_data)[:300]}...")
        
        # Transformar as chaves do JSON primeiro
        transformed_json = transform_keys(json_data)
        
        # Log para diagnóstico dos dados transformados (limitado)
        print(f"DEBUG: Estrutura JSON transformada: {json.dumps(transformed_json)[:300]}...")
        
        # Usar o ID do registro transformado
        record_id = transformed_json.get('id', 'N/A')
        
        # Extrair o verbID APENAS do JSON transformado
        verb_id = None
        
        # 1. Verificar no nível principal após transformação
        if 'verbid' in transformed_json:
            verb_id = transformed_json.get('verbid')
        
        # 2. Verificar dentro do objeto 'statement.verb.id' após transformação
        elif 'statement' in transformed_json and isinstance(transformed_json['statement'], dict):
            statement = transformed_json['statement']
            if 'verb' in statement and isinstance(statement['verb'], dict):
                verb_id = statement['verb'].get('id')
        
        # 3. Verificar diretamente em 'verb.id' após transformação
        elif 'verb' in transformed_json and isinstance(transformed_json['verb'], dict):
            verb_id = transformed_json['verb'].get('id')
            
        if not verb_id:
            # Se não encontrar um verbID, apenas use "event" como tipo padrão
            print(f"WARN: Campo verbID não encontrado no JSON transformado para registro {record_id}. Usando tipo 'event'.")
            event_type = "event"
        else:
            # Extrair o tipo de evento do verbID
            event_type = get_event_type(verb_id)
            
        # Mostrar apenas campos relevantes do registro para diagnóstico
        data_info = {
            'id': record_id,
            'verbID': verb_id,
            'event_type': event_type
        }
            
        print(f"DATA: Processando registro {json.dumps(data_info)}")
        
        # Inserir no S3 em vez do Redshift
        return insert_to_s3(transformed_json, event_type)
        
    except json.JSONDecodeError as je:
        print(f"ERROR: Falha ao decodificar JSON: {str(je)}")
        print(f"DADOS RECEBIDOS: {record_data[:200]}...")  # Imprimir apenas os primeiros 200 caracteres
        return False
    except Exception as e:
        print(f"ERROR: Falha ao processar registro: {str(e)}")
        return False

def get_kinesis_records(stream_arn, shard_id, start_sequence, end_sequence):
    """
    Recupera registros do Kinesis usando informações de shard e sequência
    """
    # Extrair o nome do stream do ARN ou usar o nome específico se soubermos
    if "pas-datastream-lrs-baa" in stream_arn:
        stream_name = "pas-datastream-lrs-baa"
    else:
        stream_name = stream_arn.split('/')[-1]
    
    print(f"INFO: Nome do stream Kinesis: {stream_name}")
    
    try:
        # Configurar o cliente kinesis com timeout aumentado
        kinesis_config = boto3.session.Config(
            connect_timeout=20,     # Tempo limite de conexão em segundos
            read_timeout=30,        # Tempo limite de leitura em segundos
            retries={'max_attempts': 5}  # Número máximo de tentativas
        )
        
        # Criando um novo cliente com a configuração personalizada
        kinesis_client = boto3.client('kinesis', config=kinesis_config)
        
        print(f"INFO: Tentando obter shard iterator para stream: {stream_name}, shard: {shard_id}")
        
        # Obter um shard iterator
        shard_iterator_response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='AT_SEQUENCE_NUMBER',
            StartingSequenceNumber=start_sequence
        )
        
        shard_iterator = shard_iterator_response['ShardIterator']
        records = []
        
        print(f"INFO: Shard iterator obtido com sucesso, buscando registros...")
        
        # Recuperar registros até atingir o número de sequência final
        while shard_iterator:
            response = kinesis_client.get_records(
                ShardIterator=shard_iterator,
                Limit=100
            )
            
            batch_records = response.get('Records', [])
            if batch_records:
                print(f"INFO: Recuperados {len(batch_records)} registros do Kinesis")
                records.extend(batch_records)
                
                # Verificar se chegamos ao número de sequência final
                for record in batch_records:
                    if record['SequenceNumber'] == end_sequence:
                        print(f"INFO: Encontrado registro final com sequência {end_sequence}")
                        return records
            else:
                print("INFO: Nenhum registro retornado na chamada de get_records")
            
            # Próximo shard iterator ou None se chegamos ao fim
            shard_iterator = response.get('NextShardIterator')
            
            # Se não há mais registros ou chegamos ao final do shard
            if not batch_records or not shard_iterator:
                print("INFO: Fim do shard ou não há mais registros")
                break
            
            # Pequena pausa para evitar throttling
            time.sleep(0.5)  # Aumentado o tempo de pausa
        
        if not records:
            print("WARN: Nenhum registro encontrado no Kinesis com as sequências especificadas")
        else:
            print(f"INFO: Total de {len(records)} registros recuperados do Kinesis")
        
        return records
            
    except Exception as e:
        print(f"ERROR: Falha ao recuperar registros do Kinesis: {str(e)}")
        import traceback
        print(f"TRACEBACK: {traceback.format_exc()}")
        return []

def process_dlq_message(message):
    """
    Processa uma mensagem da DLQ
    """
    try:
        # Converter a mensagem para JSON
        message_body = json.loads(message['Body'])
        
        # Verificar se é uma mensagem de idade excedida
        if message_body.get('requestContext', {}).get('condition') != 'RecordAgeExceeded':
            print(f"INFO: Mensagem ignorada - não é uma mensagem de idade excedida. Condição: {message_body.get('requestContext', {}).get('condition')}")
            return True
        
        # Obter informações do Kinesis
        kinesis_info = message_body.get('KinesisBatchInfo', {})
        shard_id = kinesis_info.get('shardId')
        start_sequence = kinesis_info.get('startSequenceNumber')
        end_sequence = kinesis_info.get('endSequenceNumber')
        stream_arn = kinesis_info.get('streamArn')
        
        # Verificação específica para o stream pas-datastream-lrs-baa
        if "pas-datastream-lrs-baa" not in stream_arn and stream_arn:
            print(f"WARN: Stream diferente do esperado (pas-datastream-lrs-baa): {stream_arn}")
            
        batch_size = kinesis_info.get('batchSize', 0)
        
        if not all([shard_id, start_sequence, end_sequence, stream_arn]):
            print(f"ERROR: Informações de Kinesis incompletas na mensagem: {json.dumps(kinesis_info)}")
            return True
        
        # Informações de timestamp do evento original
        arrival_time = kinesis_info.get('approximateArrivalOfFirstRecord', 'N/A')
        
        print(f"INFO: Recuperando {batch_size} registros do Kinesis. Shard: {shard_id}, Timestamp: {arrival_time}")
        print(f"INFO: Sequência inicial: {start_sequence}, Sequência final: {end_sequence}")
        print(f"INFO: Stream ARN: {stream_arn}")
        
        # Recuperar os registros originais do Kinesis
        kinesis_records = get_kinesis_records(stream_arn, shard_id, start_sequence, end_sequence)
        
        if not kinesis_records:
            print("ERROR: Não foi possível recuperar registros do Kinesis - verifique a conectividade e período de retenção")
            # Retornando True para que a mensagem seja removida da fila,
            # evitando processamento repetido de mensagens que não podem ser recuperadas
            return True
        
        print(f"SUCCESS: Recuperados {len(kinesis_records)} registros do Kinesis")
        
        # Processar cada registro
        success_count = 0
        for record in kinesis_records:
            data = record.get('Data')
            if data:
                if process_record(data):
                    success_count += 1
            else:
                print("ERROR: Campo 'Data' não encontrado no registro do Kinesis")
        
        print(f"SUMMARY: Processados {success_count} de {len(kinesis_records)} registros com sucesso")
        return True
        
    except json.JSONDecodeError as je:
        print(f"ERROR: Falha ao decodificar JSON da mensagem SQS: {str(je)}")
        print(f"BODY: {message.get('Body', '')[:200]}...")  # Imprimir parte do body para diagnóstico
        return False
    except Exception as e:
        print(f"ERROR: Falha ao processar mensagem da DLQ: {str(e)}")
        import traceback
        print(f"TRACEBACK: {traceback.format_exc()}")
        return False

def process_sqs_event(event):
    """
    Processa eventos SQS recebidos pela função Lambda
    """
    success_count = 0
    record_count = 0
    
    # Percorrer os registros SQS
    for record in event.get('Records', []):
        record_count += 1
        try:
            # Imprimir informações sobre o registro para diagnóstico
            print(f"INFO: Processando registro SQS ID: {record.get('messageId', 'N/A')}")
            
            # Obter mensagem SQS
            message = {
                'Body': record.get('body', '{}'),
                'MessageId': record.get('messageId', ''),
                'ReceiptHandle': record.get('receiptHandle', '')
            }
            
            # Processar a mensagem
            if process_dlq_message(message):
                # Se processada com sucesso, remover da DLQ
                sqs.delete_message(
                    QueueUrl=DLQ_URL,
                    ReceiptHandle=record.get('receiptHandle', '')
                )
                print(f"SUCCESS: Mensagem {message['MessageId']} processada e removida da DLQ")
                success_count += 1
            else:
                print(f"ERROR: Falha ao processar mensagem {message['MessageId']}")
        except Exception as e:
            print(f"ERROR: Falha ao processar evento SQS: {str(e)}")
    
    return success_count, record_count

def lambda_handler(event, context):
    """
    Função handler para o AWS Lambda
    """
    start_time = datetime.now()
    
    try:
        print(f"INFO: Evento recebido: {json.dumps(event)}")
        
        if 'Records' in event and len(event['Records']) > 0:
            # Parece ser um evento do SQS
            record_count = len(event.get('Records', []))
            print(f"INFO: Iniciando processamento de {record_count} mensagens SQS")
            
            # Configurar o cliente kinesis com timeout aumentado para toda a execução
            global kinesis
            kinesis_config = boto3.session.Config(
                connect_timeout=20,     # Tempo limite de conexão aumentado
                read_timeout=30,        # Tempo limite de leitura aumentado
                retries={'max_attempts': 5}  # Mais tentativas
            )
            
            # Obter a região da AWS atual
            session = boto3.session.Session()
            current_region = session.region_name
            print(f"INFO: Região AWS atual: {current_region}")
            
            # Inicializar cliente Kinesis com configuração personalizada
            kinesis = boto3.client('kinesis', config=kinesis_config, region_name=current_region)
            
            # Definir timeout mais curto para a função (pode ser removido da VPC)
            # Esta função não precisa mais de 3 minutos para executar
            timeout_at = start_time.timestamp() + 180  # 3 minutos de timeout
            
            success_count, total_count = process_sqs_event(event)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"SUMMARY: Processadas {success_count} de {total_count} mensagens com sucesso. Duração: {duration:.2f} segundos")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Processados {success_count} de {total_count} mensagens com sucesso',
                    'duration': f'{duration:.2f} segundos'
                })
            }
        else:
            # Evento não reconhecido ou sem registros
            print("INFO: Evento sem registros para processar")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Nenhum registro para processar'
                })
            }
    
    except Exception as e:
        print(f"ERROR: Falha na execução do lambda_handler: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Erro: {str(e)}'
            })
        }