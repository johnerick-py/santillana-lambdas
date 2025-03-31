import json
import base64
import logging
import os
import boto3
import uuid
import time
import random
import psycopg2
from psycopg2 import pool
from datetime import datetime

# Configurações de log e S3
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
BUCKET_NAME = 'pas-prod-landing-zone'

# Prefixos S3 conforme os requisitos
KEY_PREFIX_RAW   = 'streaming/validacao-wicco/'
KEY_PREFIX_EVENT_LOG = 'streaming/lrs_baa/event/'
KEY_PREFIX_UNKNOWN   = 'streaming/lrs_baa/verbid_unknown/'
KEY_PREFIX_PROCESSED = 'streaming/lrs_baa/verb_id/'
KEY_PREFIX_ERROR     = 'streaming/lrs_baa/event_error/redshift_conn/'

KINESIS_NAME = "pas-datastream-lrs-baa"


lrs_object_http_columns = [
    "dt_load",                   #0
    "id_system",                 #1
    "lrs_id",                    #2
    "client_id",                 #3
    "statement_id",              #4
    "object_id",                 #5
    "action_type_id",            #6
    "resource_id",               #7
    "page_id",                   #8
    "unit_id",                   #9
    "school_class_id",           #10
    "student_id",                #11
    "school_id",                 #12
    "role_id",                   #13
    "subject_grade_id",          #14
    "section_subject_id",        #15
    "grade_id",                  #16
    "source_id",                 #17
    "type_evaluation",           #18
    "planning_id",               #19
    "moment_code",               #20
    "unit_code",                 #21
    "group_id",                  #22
    "resource_ref_id",           #23
    "activity_id",               #24
    "learning_unit_id",          #25
    "learning_objective_id",     #26
    "page_ref",                  #27
    "type_intelligence",         #28
    "level_code",                #29
    "learning_level_id",         #30
    "adventure_code",            #31
    "activity_type",             #32
    "act_type",                  #33
    "learning_objective_id_api", #34
    "achievement_code"           #35
]



lrs_results_columns = [
    "dt_load",          #0
    "id_system",        #1
    "lrs_id",           #2
    "client_id",        #3
    "statement_id",     #4
    "success_status",   #5
    "response",         #6
    "duration",         #7
    "score_scaled",     #8
    "score_raw",        #9
    "score_min",        #10
    "score_max",        #11
    "completion",       #12
    "interaction_type", #13
    "extensions"        #14
]



lrs_result_scorm = [
    "dt_load",                   #0
    "id_system",                 #1
    "lrs_id",                    #2
    "client_id",                 #3
    "statement_id",              #4
    "correct_responses_pattern", #5
    "response",                  #6
    "multiple_recording",        #7
    "image"                      #8
]


dim_verb = [
    "dt_load",       #0
    "id_system",     #1
    "lrs_id",        #2
    "client_id",     #3
    "statement_id",  #4
    "verb_id",       #5
    "display_de_de", #6
    "display_en_us", #7
    "display_fr_fr", #8
    "display_es_es", #9
    "display_und",   #10
    "display_es"     #11
]



dim_statement = [
    "dt_load",            #0
    "id_system",          #1
    "lrs_id",             #2
    "client_id",          #3
    "statement_id",       #4
    "date_insert",        #5 ["date_insert"]
    "active_status",      #6 ["active"]
    "verb_id",            #7 ["verbID"]
    "activity_id",        #8 ["activityID"]
    "related_agents",     #9 ["relatedAgents"]
    "related_activities", #10 ["relatedActivities"]
    "date_stored",        #11 ["statement"]["stored"]
    "voided_status",      #12 [voided]
    "dt_time"             #13 ["statement"]["timestamp"]
]

def build_processed_record(input_data, db_columns, exclude_columns=['dt_load', 'dt_end']):
    """
    Monta o processed_record na ordem das colunas do banco, preenchendo valores ausentes com None,
    e retorna um dicionário com as colunas e valores para a query dinâmica.

    :param input_data: Lista de tuplas [(coluna_nome, valor)] com os dados do JSON.
    :param db_columns: Lista de colunas na tabela do banco de dados.
    :param exclude_columns: Lista de colunas que devem ser ignoradas.
    :return: Tuple (processed_record, columns_values_dict).
    """
    if exclude_columns is None:
        exclude_columns = []

    # Converte a lista de entrada em um dicionário para acesso eficiente
    input_dict = dict(input_data)

    # Monta o processed_record na ordem das colunas, ignorando as excluídas
    processed_record = tuple(
        input_dict.get(column, None) if column not in exclude_columns else None
        for column in db_columns
    )

    # Cria o dicionário de colunas e valores para a query
    columns_values_dict = {
        column: input_dict[column]
        for column in db_columns
        if column in input_dict and column not in exclude_columns
    }

    return processed_record, columns_values_dict

# Configuração da conexão Redshift com SSL e parâmetros de keepalive
redshift_config = {
    'dbname': 'datalake',
    'user': 'admin',
    'password': 'IVDMBvybjo842++',
    'host': 'pas-prod-redshift-workgroup.888577054267.us-east-1.redshift-serverless.amazonaws.com',
    'port': 5439,
    'sslmode': 'require',
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5
}

# Pool de conexões (5 mín, 20 máx)
connection_pool = psycopg2.pool.SimpleConnectionPool(5, 20, **redshift_config)

# Buffer para lotes de inserção
batch_size = 1  # Define o tamanho do lote
buffer = []


def insert_batch_to_redshift_dynamic(records_with_columns, table_name, max_retries=3):
    """
    Insere um lote de registros no Redshift com colunas dinâmicas.
    Em caso de erro de SSL, fecha a conexão problemática, reestabelece e tenta reinserir.
    """
    retries = 0
    while retries < max_retries:
        conn = None
        try:
            conn = connection_pool.getconn()
            if conn.closed != 0:
                print("Conexão fechada. Obtendo nova conexão.")
                conn = connection_pool.getconn()
            cursor = conn.cursor()
            for processed_record, columns_values in records_with_columns:
                columns = ', '.join(columns_values.keys())
                placeholders = ', '.join(['%s'] * len(columns_values))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
                cursor.execute(query, list(columns_values.values()))
            conn.commit()
            print(f"Inseridos {len(records_with_columns)} registros na tabela {table_name}.")
            break  # Inserção bem-sucedida, sai do loop de retry
        except Exception as e:
            if "SSL connection has been closed unexpectedly" in str(e):
                print("Conexão fechada inesperadamente. Tentando reestabelecer a conexão e reinserir.")
                if conn:
                    conn.close()  # Fecha a conexão problemática
                retries += 1
                time.sleep(1)  # Aguarda um pouco antes de tentar novamente
            else:
                print(f"Erro ao inserir no Redshift: {e}")
                break
        finally:
            if conn:
                try:
                    connection_pool.putconn(conn)
                except Exception as e:
                    print("Erro ao devolver conexão para o pool:", e)
    else:
        print("Máximo de tentativas atingido. Dados não foram inseridos.")

# Buffers para os inserts (chave: nome da tabela, valor: lista de tuplas (processed_record, columns_values))
buffers = {
    "gold.dim_lrs_verb": [],
    "gold.dim_lrs_statement": [],
    "gold.dim_lrs_object_http": [],
    "gold.dim_lrs_result": [],
    "gold.dim_lrs_result_scorm": [],
    "infrastructure.kinesis_log": []
}

# ----------------------------------------------------------------
# Função para salvar um JSON no S3 e retornar a URI
def upload_json_to_s3(json_data, key_prefix, filename=None):
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
    logger.info("Arquivo salvo no S3: %s", s3_uri)
    return s3_uri

# ----------------------------------------------------------------
# Função para salvar o evento cru (payload original) e retornar a URI para kinesis_log
def save_raw_event(payload_json, record):
    now = datetime.utcnow()
    # Monta o caminho conforme mes/dia/hora atual
    key_prefix = f"{KEY_PREFIX_EVENT_LOG}{now.strftime('%m')}/{now.strftime('%d')}/{now.strftime('%H')}/"
    # Nome do arquivo: partition_key_sequence_number.json
    filename = f"{record['partitionKey']}_{record['sequenceNumber']}.json"
    s3_uri = upload_json_to_s3(payload_json, key_prefix, filename)
    return s3_uri

# ----------------------------------------------------------------
# Funções de transformação para cada verbID.
# Cada função deve receber o payload (já convertido em dict) e retornar um dicionário
# com os registros processados para cada tabela.
def process_answered_event(payload):
     # Transformação para 'answered'
    
    processed = {}

    processed['gold.dim_lrs_statement'] = build_processed_record(
            [
                (dim_statement[1], payload.get("id", None)),  # id_system
                (dim_statement[2], payload.get("lrs_id", None)),  # lrs_id
                (dim_statement[3], payload.get("client_id", None)),  # client_id
                (dim_statement[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (dim_statement[5], payload.get("date_insert", None)),  # date_insert
                (dim_statement[6], payload.get("active", None)),  # active_status
                (dim_statement[7], payload.get("verbID", None)),  # verb_id
                (dim_statement[8], payload.get("activityID", None)),  # activity_id
                (
                    dim_statement[9],
                    ",".join(payload.get("relatedAgents", []))
                    if isinstance(payload.get("relatedAgents", []), list)
                    else payload.get("relatedAgents", None)
                ),  # related_agents
                (
                    dim_statement[10],
                    ",".join(payload.get("relatedActivities", []))
                    if isinstance(payload.get("relatedActivities", []), list)
                    else payload.get("relatedActivities", None)
                ),  # related_activities
                (dim_statement[11], payload.get("statement", {}).get("stored", None)),  # date_stored
                (dim_statement[12], payload.get("voided", None)),  # voided_status
                (dim_statement[13], payload.get("statement", {}).get("timestamp", None))  # dt_time
            ],
            dim_statement
        )
    

    processed['gold.dim_lrs_verb'] =  build_processed_record(
            [
                (dim_verb[1],  payload.get("id", None)),  # id_system
                (dim_verb[2],  payload.get("lrs_id", None)),  # lrs_id
                (dim_verb[3],  payload.get("client_id", None)),  # client_id
                (dim_verb[4],  payload.get("statement", {}).get("id", None)),  # statement_id
                (dim_verb[5],  payload.get("statement", {}).get("verb", {}).get("id", None)),  # verb_id
                (dim_verb[10], payload.get("statement", {}).get("verb", {}).get("display", {}).get("und", None))  # display_und
            ],
            dim_verb
        )
    

    # Acessa de forma segura os dados aninhados usando .get e retornando {} quando não existir
    payload_extensions = payload.get("statement", {}).get("object", {}).get("definition", {}).get("extensions", {}).get("http://act_type", {})

    processed['gold.dim_lrs_object_http']  = build_processed_record(
        [
            (lrs_object_http_columns[1], payload.get("id", None)),  # id_system
            (lrs_object_http_columns[2], payload.get("lrs_id", None)),  # lrs_id
            (lrs_object_http_columns[3], payload.get("client_id", None)),  # client_id
            (lrs_object_http_columns[4], payload.get("statement", {}).get("id", None)),  # statement_id
            (lrs_object_http_columns[5], payload.get("statement", {}).get("object", {}).get("id", None)),  # object_id
            (lrs_object_http_columns[23], payload_extensions.get("resourceRefId", None)),  # resource_ref_id
            (lrs_object_http_columns[7], payload_extensions.get("resourceId", None)),  # resource_id
            (lrs_object_http_columns[11], payload_extensions.get("studentRefId", None)),  # student_id
            (lrs_object_http_columns[13], payload_extensions.get("rolId", None)),  # role_id
            (lrs_object_http_columns[31], payload_extensions.get("adventureCode", None)),  # adventure_code
            (lrs_object_http_columns[17], payload_extensions.get("sourceRefId", None)),  # source_id
            (lrs_object_http_columns[25], payload_extensions.get("learningUnitRefId", None)),  # learning_unit_id
            (lrs_object_http_columns[15], payload_extensions.get("sectionSubjectRefId", None)),  # section_subject_id
            (lrs_object_http_columns[10], payload_extensions.get("schoolClassId", None)),  # school_class_id
            (lrs_object_http_columns[14], payload_extensions.get("subjectGradeRefId", None)),  # subject_grade_id
            (lrs_object_http_columns[12], payload_extensions.get("schoolRefId", None)),  # school_id
        ], 
        lrs_object_http_columns
    )

    # Supondo que o payload está armazenado na variável "data"
    interaction_type = payload.get("statement", {}) \
                        .get("object", {}) \
                        .get("definition", {}) \
                        .get("interactionType", None)

    if interaction_type == "long-fill-in":
        def split_value(value, max_length=64000):
            """
            Se value for uma string com mais de max_length caracteres,
            ela é dividida em uma lista de strings com tamanho máximo de max_length.
            Se não for string ou não exceder o limite, retorna o valor original.
            """
            if value is None or not isinstance(value, str):
                return value
            if len(value) <= max_length:
                return value
            return [value[i:i+max_length] for i in range(0, len(value), max_length)]
        

        # Obtém o valor de "response" de forma segura
        response_raw = payload.get("statement", {}) \
                        .get("result", {}) \
                        .get("extensions", {}) \
                        .get("http://scorm&46;com/extensions/usa-data", {}) \
                        .get("response", None)

        # Aplica a função para garantir que o valor não ultrapasse 64.000 caracteres
        response_value_split = split_value(response_raw, 64000)

        processed['gold.dim_lrs_result_scorm'] = build_processed_record(
            [
                (lrs_result_scorm[1], payload.get("id", None)),  # id_system
                (lrs_result_scorm[2], payload.get("lrs_id", None)),  # lrs_id
                (lrs_result_scorm[3], payload.get("client_id", None)),  # client_id
                (lrs_result_scorm[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (lrs_result_scorm[5], payload.get("statement", {})
                    .get("result", {})
                    .get("extensions", {})
                    .get("http://scorm&46;com/extensions/usa-data", {})
                    .get("correctResponsesPattern", None)),  # correct_responses_pattern
                (lrs_result_scorm[6], response_value_split),  # response
                (lrs_result_scorm[7], payload.get("statement", {})
                    .get("result", {})
                    .get("extensions", {})
                    .get("http://scorm&46;com/extensions/usa-data", {})
                    .get("multipleRecording", None)),  # multiple_recording
            ],
            lrs_result_scorm
        )
        print("resposta aberta")
    elif interaction_type == "matching":
        
        processed['gold.dim_lrs_result_scorm'] = build_processed_record(
            [
                (lrs_result_scorm[1], payload.get("id", None)),  # id_system
                (lrs_result_scorm[2], payload.get("lrs_id", None)),  # lrs_id
                (lrs_result_scorm[3], payload.get("client_id", None)),  # client_id
                (lrs_result_scorm[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (lrs_result_scorm[5], payload.get("statement", {}).get("result", {}).get("extensions", {}).get("http://scorm&46;com/extensions/usa-data", {}).get("correctResponsesPattern", None)),  # correct_responses_pattern
                (lrs_result_scorm[6], payload.get("statement", {}).get("result", {}).get("extensions", {}).get("http://scorm&46;com/extensions/usa-data", {}).get("response", None)),  # response
                (lrs_result_scorm[7], payload.get("statement", {}).get("result", {}).get("extensions", {}).get("http://scorm&46;com/extensions/usa-data", {}).get("multipleRecording", None)),  # multiple_recording
            ],
            lrs_result_scorm
        )
    else:
        print("Tipo de interação não reconhecido")

    return processed

def process_experienced_event(payload):
    # Transformação para 'experienced'
    processed = {}
    # Supondo que o payload esteja armazenado na variável "data"
    definition = payload.get("statement", {}).get("object", {}).get("definition", {})
    extensions = definition.get("extensions", {})

    if "http://ids" in extensions:
        processed['gold.dim_lrs_statement'] = build_processed_record(
                [
                    (dim_statement[1], payload.get("id", None)),  # id_system
                    (dim_statement[2], payload.get("lrs_id", None)),  # lrs_id
                    (dim_statement[3], payload.get("client_id", None)),  # client_id
                    (dim_statement[4], payload.get("statement", {}).get("id", None)),  # statement_id
                    (dim_statement[5], payload.get("date_insert", None)),  # date_insert
                    (dim_statement[6], payload.get("active", None)),  # active_status
                    (dim_statement[7], payload.get("verbID", None)),  # verb_id
                    (dim_statement[8], payload.get("activityID", None)),  # activity_id
                    (
                        dim_statement[9],
                        ",".join(payload.get("relatedAgents", []))
                        if isinstance(payload.get("relatedAgents", []), list)
                        else payload.get("relatedAgents", None)
                    ),  # related_agents
                    (
                        dim_statement[10],
                        ",".join(payload.get("relatedActivities", []))
                        if isinstance(payload.get("relatedActivities", []), list)
                        else payload.get("relatedActivities", None)
                    ),  # related_activities
                    (dim_statement[11], payload.get("statement", {}).get("stored", None)),  # date_stored
                    (dim_statement[12], payload.get("voided", None)),  # voided_status
                    (dim_statement[13], payload.get("statement", {}).get("timestamp", None))  # dt_time
                ],
                dim_statement
            )
        

        # Acessa de forma segura os dados aninhados usando .get e retornando {} quando não existir
        payload_ids = payload.get("statement", {}).get("object", {}).get("definition", {}).get("extensions", {}).get("http://ids", {})

        processed['gold.dim_lrs_object_http']  = build_processed_record(
            [
                (lrs_object_http_columns[1],  payload.get("id", None)),  # id_system
                (lrs_object_http_columns[2],  payload.get("lrs_id", None)),  # lrs_id
                (lrs_object_http_columns[3],  payload.get("client_id", None)),  # client_id
                (lrs_object_http_columns[4],  payload.get("statement", {}).get("id", None)),  # statement_id
                (lrs_object_http_columns[5],  payload.get("statement", {}).get("object", {}).get("id", None)),  # object_id
                (lrs_object_http_columns[23], payload_ids.get("resourceRefId", None)),  # resource_ref_id
                (lrs_object_http_columns[7],  payload_ids.get("resourceId", None)),  # resource_id
                (lrs_object_http_columns[11], payload_ids.get("studentRefId", None)),  # student_id
                (lrs_object_http_columns[13], payload_ids.get("rolId", None)),  # role_id
                (lrs_object_http_columns[31], payload_ids.get("adventureCode", None)),  # adventure_code
                (lrs_object_http_columns[17], payload_ids.get("sourceRefId", None)),  # source_id
                (lrs_object_http_columns[25], payload_ids.get("learningUnitRefId", None)),  # learning_unit_id
                (lrs_object_http_columns[15], payload_ids.get("sectionSubjectRefId", None)),  # section_subject_id
                (lrs_object_http_columns[10], payload_ids.get("schoolClassId", None)),  # school_class_id
                (lrs_object_http_columns[14], payload_ids.get("subjectGradeRefId", None)),  # subject_grade_id
                (lrs_object_http_columns[12], payload_ids.get("schoolRefId", None)),  # school_id
            ], 
            lrs_object_http_columns
        )


        processed['gold.dim_lrs_verb'] =  build_processed_record(
                [
                    (dim_verb[1],  payload.get("id", None)),  # id_system
                    (dim_verb[2],  payload.get("lrs_id", None)),  # lrs_id
                    (dim_verb[3],  payload.get("client_id", None)),  # client_id
                    (dim_verb[4],  payload.get("statement", {}).get("id", None)),  # statement_id
                    (dim_verb[5],  payload.get("statement", {}).get("verb", {}).get("id", None)),  # verb_id
                    (dim_verb[10], payload.get("statement", {}).get("verb", {}).get("display", {}).get("und", None))  # display_und
                ],
                dim_verb
            )
    else:
        print("não tem")
    return processed

def process_attempted_event(payload):
    # Transformação para 'attempted'
    processed = {}
    return processed

def process_terminated_event(payload):
    # Transformação para 'terminated'
    processed = {}
    processed['gold.dim_lrs_statement'] = build_processed_record(
            [
                (dim_statement[1], payload.get("id", None)),  # id_system
                (dim_statement[2], payload.get("lrs_id", None)),  # lrs_id
                (dim_statement[3], payload.get("client_id", None)),  # client_id
                (dim_statement[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (dim_statement[5], payload.get("date_insert", None)),  # date_insert
                (dim_statement[6], payload.get("active", None)),  # active_status
                (dim_statement[7], payload.get("verbID", None)),  # verb_id
                (dim_statement[8], payload.get("activityID", None)),  # activity_id
                (
                    dim_statement[9],
                    ",".join(payload.get("relatedAgents", []))
                    if isinstance(payload.get("relatedAgents", []), list)
                    else payload.get("relatedAgents", None)
                ),  # related_agents
                (
                    dim_statement[10],
                    ",".join(payload.get("relatedActivities", []))
                    if isinstance(payload.get("relatedActivities", []), list)
                    else payload.get("relatedActivities", None)
                ),  # related_activities
                (dim_statement[11], payload.get("statement", {}).get("stored", None)),  # date_stored
                (dim_statement[12], payload.get("voided", None)),  # voided_status
                (dim_statement[13], payload.get("statement", {}).get("timestamp", None))  # dt_time
            ],
            dim_statement
        )
    

    processed['gold.dim_lrs_verb'] =  build_processed_record(
            [
                (dim_verb[1],  payload.get("id", None)),  # id_system
                (dim_verb[2],  payload.get("lrs_id", None)),  # lrs_id
                (dim_verb[3],  payload.get("client_id", None)),  # client_id
                (dim_verb[4],  payload.get("statement", {}).get("id", None)),  # statement_id
                (dim_verb[5],  payload.get("statement", {}).get("verb", {}).get("id", None)),  # verb_id
                (dim_verb[10], payload.get("statement", {}).get("verb", {}).get("display", {}).get("und", None))  # display_und
            ],
            dim_verb
        )
    

    # Acessa de forma segura os dados aninhados usando .get e retornando {} quando não existir
    payload_ids = payload.get("statement", {}).get("object", {}).get("definition", {}).get("extensions", {}).get("http://ids", {})

    processed['gold.dim_lrs_object_http']  = build_processed_record(
        [
            (lrs_object_http_columns[1], payload.get("id", None)),  # id_system
            (lrs_object_http_columns[2], payload.get("lrs_id", None)),  # lrs_id
            (lrs_object_http_columns[3], payload.get("client_id", None)),  # client_id
            (lrs_object_http_columns[4], payload.get("statement", {}).get("id", None)),  # statement_id
            (lrs_object_http_columns[5], payload.get("statement", {}).get("object", {}).get("id", None)),  # object_id
            (lrs_object_http_columns[23], payload_ids.get("resourceRefId", None)),  # resource_ref_id
            (lrs_object_http_columns[7], payload_ids.get("resourceId", None)),  # resource_id
            (lrs_object_http_columns[11], payload_ids.get("studentRefId", None)),  # student_id
            (lrs_object_http_columns[13], payload_ids.get("rolId", None)),  # role_id
            (lrs_object_http_columns[31], payload_ids.get("adventureCode", None)),  # adventure_code
            (lrs_object_http_columns[17], payload_ids.get("sourceRefId", None)),  # source_id
            (lrs_object_http_columns[25], payload_ids.get("learningUnitRefId", None)),  # learning_unit_id
            (lrs_object_http_columns[15], payload_ids.get("sectionSubjectRefId", None)),  # section_subject_id
            (lrs_object_http_columns[10], payload_ids.get("schoolClassId", None)),  # school_class_id
            (lrs_object_http_columns[14], payload_ids.get("subjectGradeRefId", None)),  # subject_grade_id
            (lrs_object_http_columns[12], payload_ids.get("schoolRefId", None)),  # school_id
        ], 
        lrs_object_http_columns
    )
    




    if payload.get("statement", {}).get("result") is not None:

        processed['gold.dim_lrs_result'] = build_processed_record(
                [
                    (lrs_results_columns[1],  payload.get("id", None)),  # id_system
                    (lrs_results_columns[2],  payload.get("lrs_id", None)),  # lrs_id
                    (lrs_results_columns[3],  payload.get("client_id", None)),  # client_id
                    (lrs_results_columns[4],  payload.get("statement", {}).get("id", None)),  # statement_id
                    (lrs_results_columns[5],  payload.get("statement", {}).get("result", {}).get("completion", None)),  # success_status
                    (lrs_results_columns[7],  payload.get("statement", {}).get("result", {}).get("duration", None)),  # duration
                    (lrs_results_columns[8],  payload.get("statement", {}).get("result", {}).get("score", {}).get("scaled", None)),  # score_scaled
                    (lrs_results_columns[9],  payload.get("statement", {}).get("result", {}).get("score", {}).get("raw", None)),  # score_raw
                    (lrs_results_columns[10], payload.get("statement", {}).get("result", {}).get("score", {}).get("min", None)),  # score_min
                    (lrs_results_columns[11], payload.get("statement", {}).get("result", {}).get("score", {}).get("max", None)),  # score_max
                    (lrs_results_columns[12], payload.get("statement", {}).get("result", {}).get("completion", None)),  # completion
                    (lrs_results_columns[13], payload.get("statement", {}).get("object", {}).get("definition", {}).get("interactionType", None)),  # interaction_type
                    (lrs_results_columns[14], 'http://scorm&46;com/extensions/usa-data')  # extensions
                ],
                lrs_results_columns
            )
    else:
        pass

    print(processed)
    return processed

# ----------------------------------------------------------------
# Função dispatcher: chama a função de processamento conforme o verbID
def process_event_by_verb(payload):
    verb_id = payload.get("verbID")
    if verb_id == "http://adlnet.gov/expapi/verbs/answered":
        return process_answered_event(payload)
    elif verb_id == "http://adlnet.gov/expapi/verbs/experienced":
        return process_experienced_event(payload)
    elif verb_id == "http://adlnet.gov/expapi/verbs/attempted":
        return process_attempted_event(payload)
    elif verb_id == "http://adlnet.gov/expapi/verbs/terminated":
        return process_terminated_event(payload)
    else:
        # Retorna None para indicar que é um verbID desconhecido
        return None

# ----------------------------------------------------------------
# Função para acumular os registros de cada tabela (buffers global)
def accumulate_processed_records(processed_dict):
    # processed_dict é um dicionário com chave = nome da tabela e valor = tuple (processed_record, columns_values)
    for table, record_tuple in processed_dict.items():
        if record_tuple is not None:
            buffers[table].append(record_tuple)

# ----------------------------------------------------------------
# Função para inserir todos os buffers no Redshift (para cada tabela)
def flush_buffers_to_redshift():
    for table, records in buffers.items():
        if records:
            try:
                insert_batch_to_redshift_dynamic(records, table)
                # Limpa o buffer após inserção bem-sucedida
                buffers[table] = []
            except Exception as e:
                logger.error("Falha na inserção para a tabela %s: %s", table, e)
                # Se falhar após 3 tentativas, salva os dados do batch no S3 para auditoria
                error_filename = f"{table}_{uuid.uuid4()}.txt"
                error_path = f"{KEY_PREFIX_ERROR}{error_filename}"
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=error_path,
                    Body=json.dumps([rec[1] for rec in records], ensure_ascii=False),
                    ContentType='application/json'
                )
                logger.error("Batch salvo em S3: s3://%s/%s", BUCKET_NAME, error_path)
                # colocar para disparar uma mensagem de falha (ex.: via SNS ou CloudWatch Alarm)

# ----------------------------------------------------------------
# Função para salvar payloads processados (após todos os eventos) no S3 conforme o path final
def save_processed_payload(payload):
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
    logger.info("Payload processado salvo em: s3://%s/%s", BUCKET_NAME, key)

# ----------------------------------------------------------------
# Função para salvar evento com verbID desconhecido
def save_unknown_verb_event(payload):
    verb_id = payload.get("verbID", "unknown").split("/")[-1]
    filename = f"{verb_id}_{uuid.uuid4()}.json"
    key = f"{KEY_PREFIX_UNKNOWN}{filename}"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False),
        ContentType='application/json'
    )
    logger.info("Evento com verbID desconhecido salvo em: s3://%s/%s", BUCKET_NAME, key)

# ----------------------------------------------------------------
# Função para montar o registro para kinesis_log
def build_kinesis_log_record(record, s3_uri):
    # Extrai o shard_id a partir do eventID (split pelo ':' e pega a primeira parte)
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
    # O dt_load pode ser null para o Redshift
    return log_record

# ----------------------------------------------------------------
# Lambda handler principal
def lambda_handler(event, context):
    #logger.info("Evento recebido: %s", json.dumps(event))
    
    
    # Processa cada registro do microbatch
    for record in event:
        try:
          
            #Decodifica o payload
            encoded_data = record["data"]
            payload_str = base64.b64decode(encoded_data).decode('utf-8')
            payload_json = json.loads(payload_str)
           
            # Salva o evento CRU (raw) no S3 e captura a URI
            raw_event_s3_uri = save_raw_event(payload_json, record)
            
            
            
            # Prepara registro para tabela kinesis_log e acumula no buffer
            kinesis_log = build_kinesis_log_record(record, raw_event_s3_uri)
            buffers["infrastructure.kinesis_log"].append((None, kinesis_log))  # O processed_record pode ser None
            

            # Processa o payload conforme o verbID
            processed_dict = process_event_by_verb(payload_json)
            if processed_dict is None:
                # VerbID desconhecido: salva no diretório de verbid_unknown
                save_unknown_verb_event(payload_json)
            else:
                # Acumula os registros processados em buffers para cada tabela
                accumulate_processed_records(processed_dict)
                
                # Salva o payload processado no S3 conforme path final
                save_processed_payload(payload_json)
                
        except Exception as e:
            logger.error("Erro ao processar registro: %s", e)
    
    # Após iterar todos os registros, insere os registros acumulados no Redshift
    flush_buffers_to_redshift()
    print("chegou até aqui")
    
    return {
        "statusCode": 200,
        "body": json.dumps("Processamento concluído com sucesso!")
    }
