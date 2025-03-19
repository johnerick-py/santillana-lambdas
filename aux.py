import json
import boto3
import base64
from datetime import datetime, timezone
import psycopg2
import os
import time
import random

# Inicializar clientes
sqs = boto3.client('sqs')

# Configurações
SQS_QUEUE_URL = 'arn:aws:sqs:us-east-1:888577054267:lrs-7447-stream'  # Atualize com a URL/ARN correta da sua fila

# Configurações do Redshift
DBNAME = os.environ.get('DBNAME')
HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT', '5439')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')

# Configuração de TTL da conexão
conn_ttl = int(os.environ.get('CONN_TTL', '600'))  # TTL para a conexão em segundos, default 10 minutos

# Variável global para armazenar a conexão entre invocações da Lambda
conn = None
last_conn_time = 0

def get_connection():
    """
    Obtém uma conexão com o Redshift, reutilizando a existente se ainda estiver válida
    Implementa backoff exponencial para retry em caso de falhas
    """
    global conn, last_conn_time
    
    current_time = time.time()
    
    # Verifica se já existe uma conexão ativa e se ela ainda está dentro do TTL
    if conn and (current_time - last_conn_time) < conn_ttl:
        try:
            # Testa se a conexão ainda está ativa com uma query simples
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            cursor.fetchone()
            cursor.close()
            print("INFO: Reutilizando conexão existente com o Redshift")
            return conn
        except Exception as e:
            print(f"WARN: Conexão existente não está mais válida: {str(e)}")
            # Fecha a conexão com problema
            try:
                conn.close()
            except:
                pass
            conn = None
    
    # Se não tem conexão ou ela expirou, cria uma nova com retry e backoff exponencial
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"INFO: Criando nova conexão com o Redshift (tentativa {retry_count + 1})")
            new_conn = psycopg2.connect(
                dbname=DBNAME,
                host=HOST,
                port=PORT,
                user=USER,
                password=PASSWORD,
                # Parâmetros adicionais para melhorar a gestão de conexões
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            
            # Atualiza as variáveis globais
            conn = new_conn
            last_conn_time = current_time
            
            return conn
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"ERROR: Falha ao conectar ao Redshift após {max_retries} tentativas: {str(e)}")
                raise e
            
            # Backoff exponencial com jitter para evitar tempestade de reconexões
            sleep_time = (2 ** retry_count) + (random.randint(0, 1000) / 1000)
            print(f"WARN: Falha na conexão, tentando novamente em {sleep_time:.2f} segundos. Erro: {str(e)}")
            time.sleep(sleep_time)

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
    Insere o JSON processado na tabela correspondente do Redshift com retry em caso de falhas
    """
    cursor = None
    connection = None
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Obter conexão do pool
            connection = get_connection()
            cursor = connection.cursor()
            
            # Converter JSON para string e escapar caracteres especiais para SQL
            json_string = json.dumps(json_data).replace("'", "''")
            
            # Inserir usando JSON_PARSE para processar diretamente o JSON
            query = f"INSERT INTO stream_raw.{table_name} (event) VALUES(JSON_PARSE('{json_string}'))"
            
            # Executar query
            cursor.execute(query)
            connection.commit()
            
            if cursor:
                cursor.close()
                
            # Obter o timestamp atual
            current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            
            # Imprimir mensagem de sucesso com timestamp
            print(f"SUCCESS: [{current_time}] Registro inserido na tabela {table_name}")
            return True
            
        except psycopg2.Error as e:
            retry_count += 1
            
            # Fecha recursos abertos
            if cursor:
                cursor.close()
            
            # Verifica se é um erro de conexão
            if e.pgcode is None or e.pgcode in ('08000', '08003', '08006', '08001', '08004', '08007', '08P01'):
                print(f"WARN: Erro de conexão durante a inserção: {str(e)}. Tentando novamente.")
                
                # Força o fechamento da conexão problemática
                global conn, last_conn_time
                try:
                    if conn:
                        conn.close()
                except:
                    pass
                    
                conn = None
                last_conn_time = 0
                
                # Backoff antes de tentar novamente
                sleep_time = 1 * retry_count
                time.sleep(sleep_time)
            else:
                # Para outros erros que não são de conexão, faz rollback e propaga o erro
                if connection:
                    connection.rollback()
                    
                if retry_count >= max_retries:
                    print(f"ERROR: Falha na inserção no Redshift após {max_retries} tentativas: {str(e)}")
                    return False
                    
                # Se ainda temos retries, tenta novamente após um backoff
                sleep_time = 0.5 * retry_count
                time.sleep(sleep_time)
    
    return False

def process_record(json_data, message_id):
    """
    Processa um registro JSON e insere no Redshift
    """
    try:
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
            'verbID': verb_id,
            'message_id': message_id
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
            table_name = 'lrs_event_all'
            
        # Transformar as chaves do JSON
        transformed_json = transform_keys(json_data)
        
        # Inserir no Redshift
        return insert_to_redshift(transformed_json, table_name)
        
    except Exception as e:
        print(f"ERROR: Falha ao processar registro: {str(e)}")
        return False

def extract_data_from_sqs_message(message):
    """
    Extrai dados encapsulados em mensagens SQS que contêm eventos do Kinesis
    """
    try:
        # Obter o corpo da mensagem SQS
        message_body = json.loads(message.get('body', '{}'))
        message_id = message.get('messageId', 'unknown')
        
        # Obter o campo 'data' do evento Kinesis (codificado em base64)
        encoded_data = message_body.get('data', '')
        
        # Decodificar o campo 'data' de base64
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        
        # Converter a string decodificada para JSON
        json_data = json.loads(decoded_data)
        
        return json_data, message_id
        
    except json.JSONDecodeError as e:
        print(f"ERROR: Falha ao decodificar JSON: {str(e)}")
        return None, message_id
    except Exception as e:
        print(f"ERROR: Falha ao extrair dados da mensagem: {str(e)}")
        return None, message_id

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
            # Extrair dados do evento Kinesis encapsulado na mensagem SQS
            json_data, message_id = extract_data_from_sqs_message(record)
            
            if not json_data:
                print(f"ERROR: Não foi possível extrair dados da mensagem")
                continue
            
            # Processar os dados e inserir no Redshift
            if process_record(json_data, message_id):
                # Se processada com sucesso
                print(f"SUCCESS: Mensagem {message_id} processada com sucesso")
                success_count += 1
            else:
                print(f"ERROR: Falha ao processar mensagem {message_id}")
        except Exception as e:
            print(f"ERROR: Falha ao processar evento SQS: {str(e)}")
    
    return success_count, record_count

def lambda_handler(event, context):
    """
    Função handler para o AWS Lambda
    """
    start_time = datetime.now(timezone.utc)
    req_id = context.aws_request_id if hasattr(context, 'aws_request_id') else 'unknown'
    
    try:
        print(f"INFO: Iniciando processamento - Request ID: {req_id}")
        
        if 'Records' in event and len(event['Records']) > 0:
            # Parece ser um evento do SQS
            record_count = len(event.get('Records', []))
            print(f"INFO: Iniciando processamento de {record_count} mensagens SQS")
            
            success_count, total_count = process_sqs_event(event)
            
            end_time = datetime.now(timezone.utc)
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
    finally:
        # Não fechamos a conexão no finally para permitir sua reutilização entre invocações
        print(f"INFO: Processamento finalizado. Request ID: {req_id}")