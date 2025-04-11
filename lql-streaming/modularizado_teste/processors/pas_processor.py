"""
Módulo que contém os processadores específicos para cada tabela definida em table_schema.py.
"""
import time
import logging
from utils.redshift_utils import build_processed_record
from models.table_schema import (
    LQL
)

logger = logging.getLogger()


def get_keys(body, key):
    finded_key = next((item.get(key) for item in body.get('data', []) if key in item), None)
    return finded_key


def process_lql_event(payload):

    #anotação sobre os items http_flow, não foi encontrado nenhum evento com este tipo
    #e nao temos nenhum exemplo claro se vem dentro dos objetos de data ou fora, nesta caso irei inseri-los nulos

    processed = {}
    
    logger.info(f"iniciando process lql def")

    body = payload

    type_event = body.get('type', "")
    action_event = body.get('action', "")
    description_event = body.get('description', "")
    user_id = get_keys(body, 'user_id', "")
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

    
    processed['stream.lql'] = []
    data_list = payload.get("data", [])
    
    if data_list:
        logger.info(f"dentro do if")
        for data_item in data_list:
            logger.info(f"percorrendo o for")
            
            data_record = build_processed_record(
            [
                (LQL[0], data_item.get("completed_sessions", "")),
                (LQL[1], data_item.get("total_sessions" , "")),
                (LQL[2], data_item.get("cached_progress", "")),
                (LQL[3], data_item.get("title", "")),
                (LQL[4], data_item.get("status", "")),
                (LQL[5], data_item.get("updated_at", "")),
                (LQL[6], data_item.get("book_serie", "")),
                (LQL[7], data_item.get("book_teacher_id", "")),
                (LQL[8], data_item.get("resume", "")),
                (LQL[9], str(data_item.get("book_pack_ids", ""))),
                (LQL[10], data_item.get("book_isbn", "")),
                (LQL[11], data_item.get("author_surname", "")),
                (LQL[12], data_item.get("author_name", "")),
                (LQL[13], data_item.get("initiative_code", "")),
                (LQL[14], data_item.get("code", "")),
                (LQL[15], data_item.get("library", "")),
                (LQL[16], data_item.get("isbn", "")),
                (LQL[17], None), # campo info_target http_flow
                (LQL[18], None), # campo info_referrer http_flow
                (LQL[19], None), # campo info_origin http_flow
                (LQL[20], None), # campo info_school_id http_flow
                (LQL[21], None), # campo info_product_id http_flow
                (LQL[22], None), # campo info_user_id http_flow
                (LQL[23], None), # campo info_user_cookie_session_id http_flow
                (LQL[24], finished_at),
                (LQL[25], description_event),
                (LQL[26], action_event),
                (LQL[27], type_event),
                (LQL[28], data_item.get("book_pack_name", "")),
                (LQL[29], data_item.get("book_pack_id", "")),
                (LQL[30], data_item.get("product_id", "")),
                (LQL[31], group_id),
                (LQL[32], school_level_id),
                (LQL[33], session_id),
                (LQL[34], data_item.get("network_id", "")),
                (LQL[35], empresa_id),
                (LQL[36], data_item.get("name", "")),
                (LQL[37], data_item.get("description", "")),
                (LQL[38], data_item.get("type", "")),
                (LQL[39], user_id),
                (LQL[40], data_item.get("user_agent", "")),
                (LQL[41], data_item.get("locale", "")),
                (LQL[42], data_item.get("ip", "")),
                (LQL[43], school_id),
                (LQL[44], data_item.get("id", "")),
                (LQL[45], role),
                (LQL[46], data_item.get("action", "")),
                (LQL[47], data_item.get("end_date", "")),
                (LQL[48], data_item.get("start_date", "")),
                (LQL[49], None),
                (LQL[50], started_at),
            ],
            LQL
            )
            processed['stream.lql'].append(data_record)
            
    return processed 
        