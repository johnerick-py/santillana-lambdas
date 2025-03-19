
import json
import boto3
import base64
import time
from datetime import datetime
import psycopg2
import os

# Inicializar clientes
sqs = boto3.client('sqs')
kinesis = boto3.client('kinesis')

# Configurações
DLQ_URL = 'https://sqs.us-east-1.amazonaws.com/888577054267/dead-queue-lrs-baa'  # Atualize com a URL correta da sua DLQ
MAX_MESSAGES = 10  # Número de mensagens a processar de uma vez
WAIT_TIME = 20  # Tempo de espera para mensagens (em segundos)

# Configurações do Redshift
DBNAME = os.environ.get('DBNAME')
HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')

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

def get_table_by_verb_id(verb_id):
    """
    Retorna o nome da tabela correspondente ao verbID
    """
    verb_to_table = {
        "http://adlnet.gov/expapi/verbs/initialized": "lrs_events_initialized",
        "http://adlnet.gov/expapi/verbs/answered": "lrs_events_answered",
        "http://adlnet.gov/expapi/verbs/attempted": "lrs_events_attempted",
        "http://adlnet.gov/expapi/verbs/completed": "lrs_events_completed",
        "http://adlnet.gov/expapi/verbs/terminated": "lrs_events_terminated",
        "http://adlnet.gov/expapi/verbs/experienced": "lrs_events_experienced"
    }
    
    return verb_to_table.get(verb_id)

def insert_to_redshift(json_data, table_name):
    """
    Insere o JSON processado na tabela correspondente do Redshift
    """
    # Conectar ao Redshift
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=DBNAME,
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD
        )
        
        cursor = conn.cursor()
        
        # Converter JSON para string e escapar caracteres especiais para SQL
        json_string = json.dumps(json_data).replace("'", "''")
        
        # Inserir usando o comando copy para evitar problemas de parsing
        query = f"INSERT INTO stream_raw.{table_name} (event) VALUES(JSON_PARSE('{json_string}'))"
        
        # Executar query
        cursor.execute(query)
        conn.commit()
        
        # Obter o timestamp atual
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Imprimir mensagem de sucesso com timestamp
        print(f"SUCCESS: [{current_time}] Registro inserido na tabela {table_name}")
        return True
        
    except Exception as e:
        print(f"ERROR: Falha na inserção no Redshift: {str(e)}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_record(record_data):
    """
    Processa um registro do Kinesis e insere no Redshift
    """
    try:
        # Decodificar o payload
        decoded_data = record_data.decode('utf-8')
        
        # Converter para JSON
        json_data = json.loads(decoded_data)
        
        # Informações básicas para log
        record_id = json_data.get('id', 'N/A')
        
        # Extrair o verbID - verificar nos dois lugares possíveis
        verb_id = None
        
        # Primeiro no nível principal
        if 'verbID' in json_data:
            verb_id = json_data.get('verbID')
            
        # Segundo, dentro do objeto 'statement.verb.id'
        elif 'statement' in json_data and isinstance(json_data['statement'], dict):
            statement = json_data['statement']
            if 'verb' in statement and isinstance(statement['verb'], dict):
                verb_id = statement['verb'].get('id')
        
        if not verb_id:
            print(f"ERROR: Campo verbID não encontrado para registro {record_id}")
            return False
            
        # Mostrar apenas campos relevantes do registro para diagnóstico
        data_info = {
            'id': record_id,
            'verbID': verb_id
        }
        
        # Extrair data_insert se disponível
        if 'date_insert' in json_data:
            data_info['date_insert'] = json_data['date_insert']
        elif 'dateinsert' in json_data:
            data_info['date_insert'] = json_data['dateinsert']
            
        print(f"DATA: Processando registro {json.dumps(data_info)}")
        
        # Obter a tabela correspondente
        table_name = get_table_by_verb_id(verb_id)
        if not table_name:
            print(f"ERROR: Não foi encontrada tabela para o verbID: {verb_id}")
            return False
            
        # Transformar as chaves do JSON
        transformed_json = transform_keys(json_data)
        
        # Inserir no Redshift
        return insert_to_redshift(transformed_json, table_name)
        
    except Exception as e:
        print(f"ERROR: Falha ao processar registro: {str(e)}")
        return False

def get_kinesis_records(stream_arn, shard_id, start_sequence, end_sequence):
    """
    Recupera registros do Kinesis usando informações de shard e sequência
    """
    stream_name = stream_arn.split('/')[-1]
    
    try:
        # Obter um shard iterator
        shard_iterator_response = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='AT_SEQUENCE_NUMBER',
            StartingSequenceNumber=start_sequence
        )
        
        shard_iterator = shard_iterator_response['ShardIterator']
        records = []
        
        # Recuperar registros até atingir o número de sequência final
        while shard_iterator:
            response = kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=100
            )
            
            for record in response['Records']:
                records.append(record)
                if record['SequenceNumber'] == end_sequence:
                    return records
            
            # Próximo shard iterator ou None se chegamos ao fim
            shard_iterator = response.get('NextShardIterator')
            
            # Pequena pausa para evitar throttling
            if shard_iterator:
                time.sleep(0.2)
        
        return records
            
    except Exception as e:
        print(f"ERROR: Falha ao recuperar registros do Kinesis: {str(e)}")
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
            print("INFO: Mensagem ignorada - não é uma mensagem de idade excedida")
            return True
        
        # Obter informações do Kinesis
        kinesis_info = message_body.get('KinesisBatchInfo', {})
        shard_id = kinesis_info.get('shardId')
        start_sequence = kinesis_info.get('startSequenceNumber')
        end_sequence = kinesis_info.get('endSequenceNumber')
        stream_arn = kinesis_info.get('streamArn')
        batch_size = kinesis_info.get('batchSize', 0)
        
        if not all([shard_id, start_sequence, end_sequence, stream_arn]):
            print("ERROR: Informações de Kinesis incompletas na mensagem")
            return True
        
        # Informações de timestamp do evento original
        arrival_time = kinesis_info.get('approximateArrivalOfFirstRecord', 'N/A')
        
        print(f"INFO: Recuperando {batch_size} registros do Kinesis. Shard: {shard_id}, Timestamp: {arrival_time}")
        
        # Recuperar os registros originais do Kinesis
        kinesis_records = get_kinesis_records(stream_arn, shard_id, start_sequence, end_sequence)
        
        if not kinesis_records:
            print("ERROR: Não foi possível recuperar registros do Kinesis - podem estar além do período de retenção")
            return True
        
        print(f"SUCCESS: Recuperados {len(kinesis_records)} registros do Kinesis")
        
        # Processar cada registro
        success_count = 0
        for record in kinesis_records:
            data = record['Data']
            if process_record(data):
                success_count += 1
        
        print(f"SUMMARY: Processados {success_count} de {len(kinesis_records)} registros com sucesso")
        return True
        
    except Exception as e:
        print(f"ERROR: Falha ao processar mensagem da DLQ: {str(e)}")
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
        if 'Records' in event and len(event['Records']) > 0:
            # Parece ser um evento do SQS
            record_count = len(event.get('Records', []))
            print(f"INFO: Iniciando processamento de {record_count} mensagens SQS")
            
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