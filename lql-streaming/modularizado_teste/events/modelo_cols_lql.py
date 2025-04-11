import boto3
import json
import psutil
import time
import os
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
import psycopg2

# Inicializar SparkSession, GlueContext e Job
spark = SparkSession.builder \
    .appName("GlueJob") \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init("job_name")

# Configurar cliente SQS
sqs_client = boto3.client('sqs', region_name='us-east-1')

VISIBILITY_TIMEOUT = 30

# URL do SQS
sqs_url = 'https://sqs.us-east-1.amazonaws.com/888577054267/lql-streaming'

# Recuperar credenciais do Secrets Manager
def get_redshift_credentials(secret_name):
    secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
    secret_response = secrets_client.get_secret_value(SecretId=secret_name)
    secret = json.loads(secret_response['SecretString'])
    return {
        'host': secret['host'],
        'port': secret['port'],
        'dbname': secret['dbname'],
        'user': secret['username'],
        'password': secret['password']
    }

redshift_secret_name = "prod/redshift/aplication/access"
redshift_credentials = get_redshift_credentials(redshift_secret_name)

# Conexão persistente com o Redshift
redshift_connection = psycopg2.connect(
    host=redshift_credentials['host'],
    port=redshift_credentials['port'],
    dbname=redshift_credentials['dbname'],
    user=redshift_credentials['user'],
    password=redshift_credentials['password']
)
redshift_connection.autocommit = True

# Função para executar queries em lote
def execute_batch_insert(data_batch, table_name):
    try:
        # Extraímos as colunas do primeiro item para garantir a ordem
        columns = list(data_batch[0].keys())
        # Criamos a consulta de INSERT com placeholders para múltiplos valores
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        # Executamos o batch usando `execute_values`
        with redshift_connection.cursor() as cursor:
            execute_values(cursor, insert_query, [tuple(item.values()) for item in data_batch])
        
        print(f"Lote de {len(data_batch)} registros inseridos com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir batch de registros: {e}")
 
def execute_batch_delete(queries):
    try:
        with redshift_connection.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
        print("Batch de queries executado com sucesso.")
    except Exception as e:
        print(f"Erro ao executar batch de queries: {e}")
        
def get_keys(body, key):
    finded_key = next((item.get(key) for item in body.get('data', []) if key in item), None)
    return finded_key

def process_messages(messages):
    """
    Processa uma lista de mensagens para determinar a ação apropriada e atualizar os dados no banco.

    Args:
        messages (list): Lista de mensagens a serem processadas.
    """
    if not messages:
        return

    batch_data = []
    delete_queries = []

    for message in messages:
        try:
            body = message['Body']
            if isinstance(body, str):
                body = json.loads(body)
                
            
            info = body.get('info', {}) or {}
            http_flow = info.get('http_flow', {}) or {}
            
            info_user_cookie_session_id = info.get('user_cookie_session_id', 'El evento no contiene info')
            info_user_id = info.get('user_id', 'El evento no contiene info')
            info_product_id = info.get('product_id', 'El evento no contiene info')
            info_school_id = info.get('school_id', 'El evento no contiene info')
            info_origin = http_flow.get('origin', 'El evento no contiene info')
            info_referrer = http_flow.get('referrer', 'El evento no contiene info')
            info_target = http_flow.get('target', 'El evento no contiene info')

            data_list = body.get('data', [])
            type_event = body.get('type')
            action_event = body.get('action')
            description_event = body.get('description')
            user_id = get_keys(body, 'user_id')
            action = get_keys(body, 'action')
            name = get_keys(body, 'name')
            session_id = get_keys(body, 'session_id')
            group_id = get_keys(body, 'group_id')
            description = get_keys(body, 'description')
            role = get_keys(body, 'role')
            empresa_id = get_keys(body, 'empresa_id')
            school_level_id = get_keys(body, 'school_level_id')
            school_id = get_keys(body, 'school_id')
            started_at = get_keys(body, 'started_at')
            finished_at = get_keys(body, 'finished_at')

            for data in data_list:
                row = {
                    "type": data.get("type"),
                    "description": description,
                    "action": action,
                    "user_id": user_id,
                    "role": role,
                    "id": data.get("id"),
                    "name": name,
                    "empresa_id": empresa_id,
                    "network_id": data.get("network_id"),
                    "session_id": session_id,
                    "start_date": data.get("start_date"),
                    "end_date": data.get("end_date"),
                    "school_level_id": school_level_id,
                    "school_id": school_id,
                    "group_id": group_id,
                    "product_id": data.get("product_id"),
                    "book_pack_id": data.get("book_pack_id"),
                    "book_pack_name": data.get("book_pack_name"),
                    "ip_address": data.get("ip"),
                    "locale": data.get("locale"),
                    "user_agent": data.get("user_agent"),
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "type_event": type_event,
                    "action_event": action_event,
                    "description_event": description_event,
                    "isbn": data.get("isbn"),
                    "library": data.get("library"),
                    "code": data.get("code"),
                    "initiative_code": data.get("initiative_code"),
                    "author_name": data.get("author_name"),
                    "author_surname": data.get("author_surname"),
                    "book_isbn": data.get("book_isbn"),
                    "book_pack_ids" : str(data.get("book_pack_ids")),
                    "resume": data.get("resume"),
                    "book_teacher_path_id": data.get("book_teacher_id"),
                    "book_serie": data.get("book_serie"),
                    "cached_progress": data.get("cached_progress"),
                    "total_sessions": data.get("total_sessions"),
                    "completed_sessions": data.get("completed_sessions"),
                    "updated_at": data.get("updated_at"),
                    "status": data.get("status"),
                    "title": data.get("title"),
                    "info_user_cookie_session_id": info_user_cookie_session_id,
                    "info_user_id": info_user_id,
                    "info_product_id": info_product_id,
                    "info_school_id": info_school_id,
                    "info_origin": info_origin,
                    "info_referrer": info_referrer,
                    "info_target": info_target
                }

                table_name = f"stream.lql"
                key_column = "id"

                if row['action'] != "delete":
                    batch_data.append(row)
                elif row['action'] == "delete":
                    query = f"DELETE FROM {table_name} WHERE {key_column} = '{row['id']}';"
                    delete_queries.append(query)

            # Remove a mensagem da fila
            sqs_client.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=message['ReceiptHandle']
            )

        except Exception as e:
            print(f"Erro ao processar mensagem: {e}")

    if batch_data:
        execute_batch_insert(batch_data, "stream.lql")

    if delete_queries:
        execute_batch_delete(delete_queries)


# Executar batch de queries
def execute_batch_query_delete(queries):
    try:
        with redshift_connection.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
        print("Batch de queries executado com sucesso.")
    except Exception as e:
        print(f"Erro ao executar batch de queries: {e}")


def consume_sqs_messages():
    try:
        response = sqs_client.receive_message(
            QueueUrl=sqs_url,
            MaxNumberOfMessages=10,  # Máximo permitido
            WaitTimeSeconds=10,
            VisibilityTimeout=VISIBILITY_TIMEOUT
        )
        return response.get('Messages', [])
    except Exception as e:
        print(f"Erro ao consumir mensagens do SQS: {e}")
        return []

# Consumir mensagens em paralelo
def log_memory_usage():
    process = psutil.Process(os.getpid())
    print(f"Uso de memória: {process.memory_info().rss / (1024 * 1024):.2f} MB")

def delete_messages(messages):
    try:
        entries = [{"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]} for msg in messages]
        response = sqs_client.delete_message_batch(
            QueueUrl=sqs_url,
            Entries=entries
        )
        print(f"mensagens deletadas {entries}")
        if "Failed" in response:
            print(f"Falha ao deletar mensagens: {response['Failed']}")
    except Exception as e:
        print(f"Erro ao deletar mensagens em lote: {e}")

# Função para consumir e processar mensagens em paralelo
def consume_and_process():
    start_time = time.time()
    processed_count = 0

    def log_throughput():
        elapsed_time = time.time() - start_time
        print(f"Mensagens processadas: {processed_count}, Throughput: {processed_count / elapsed_time:.2f} msgs/s")

    with ThreadPoolExecutor(max_workers=16) as executor:
        while True:
            messages = consume_sqs_messages()
            if messages:
                executor.submit(process_messages, messages)
                delete_messages(messages)
                processed_count += len(messages)
                log_memory_usage()
                log_throughput()

# Iniciar o processamento
consume_and_process()
