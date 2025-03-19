import json
import boto3
import os
import psycopg2
from urllib.parse import unquote_plus
import time
import random

# Inicializar clientes
s3 = boto3.client('s3')

# Configurações do Redshift
DBNAME = os.environ.get('DBNAME')
HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT', '5439')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')

# Variável para armazenar a conexão em nível global
conn = None
last_conn_time = 0
conn_ttl = int(os.environ.get('CONN_TTL', '600'))  # TTL para a conexão em segundos, default 10 minutos

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

def insert_to_redshift(json_data):
    """
    Insere o JSON na tabela stream_raw.lrs_events_answered do Redshift
    com retry em caso de problemas de conexão
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
            query = f"INSERT INTO stream_raw.lrs_events_answered (event) VALUES(JSON_PARSE('{json_string}'))"
            
            # Executar query
            cursor.execute(query)
            connection.commit()
            
            if cursor:
                cursor.close()
                
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
                    print(f"ERROR: Falha na inserção após {max_retries} tentativas: {str(e)}")
                    raise e
                    
                # Se ainda temos retries, tenta novamente após um backoff
                sleep_time = 0.5 * retry_count
                time.sleep(sleep_time)
                
    return False

def process_s3_file(bucket, key):
    """
    Processa um arquivo JSON do S3 e insere no Redshift
    """
    try:
        # Obter o objeto do S3
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Parse do JSON
        json_data = json.loads(file_content)
        
        # Inserir dados no Redshift
        insert_success = insert_to_redshift(json_data)
        
        if insert_success:
            print(f"SUCCESS: Arquivo {key} inserido com sucesso na tabela stream_raw.lrs_events_answered")
        
        return insert_success
    except Exception as e:
        print(f"ERROR: Falha ao processar arquivo {key}: {str(e)}")
        raise e

def lambda_handler(event, context):
    """
    Função handler para o AWS Lambda
    """
    try:
        # Armazenar informações sobre o contexto para debugging
        req_id = context.aws_request_id if hasattr(context, 'aws_request_id') else 'unknown'
        remaining_time = context.get_remaining_time_in_millis() if hasattr(context, 'get_remaining_time_in_millis') else 'unknown'
        print(f"INFO: Iniciando processamento - Request ID: {req_id}, Tempo Restante: {remaining_time}ms")
        
        # Iterar pelos registros do evento S3
        for record in event['Records']:
            # Obter informações do bucket e da chave (arquivo)
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])
            
            print(f"INFO: Processando arquivo {bucket}/{key}")
            
            # Processar o arquivo e inserir no Redshift
            process_success = process_s3_file(bucket, key)
            
            if process_success:
                print(f"SUCCESS: Arquivo {bucket}/{key} processado com sucesso")
            else:
                print(f"ERROR: Falha ao processar arquivo {bucket}/{key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Processamento concluído com sucesso')
        }
    
    except Exception as e:
        print(f"ERROR: Falha na execução do lambda_handler: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro: {str(e)}')
        }
    finally:
        # Não fechamos a conexão no finally para permitir sua reutilização entre invocações
        # A conexão será reutilizada ou fechada automaticamente conforme o TTL
        print(f"INFO: Processamento finalizado. Request ID: {req_id}")