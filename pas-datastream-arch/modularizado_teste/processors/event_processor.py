"""
Módulo para processamento de eventos baseado no entityKey.
"""
import logging
from processors.pas_processor import (
    process_school_level_session_event,
    process_school_class_event,
    process_school_grade_group_event,
    process_session_event,
    process_subject_school_class_event,
    process_uno_school_event,
    process_user_event,
    process_user_group_event,
    process_user_role_event,
    process_user_school_class_event
)

logger = logging.getLogger()

# Dicionário de mapeamento entre entityKey e função processadora correspondente
ENTITY_PROCESSORS = {
    #"ARTICLE": process_article_event, # nao iremos processar este evento, desconsiderar
    #"CONTENTSCHOOLCLASS": process_content_school_class_event, # iremos usar futuramente
    #"CONTENTSCHOOLLEVELSESSION": process_school_level_session_event, # nao iremos processar este evento, desconsiderar
    #"CONTACTRELATIONSHIP": process_contact_relationship_event, # nao iremos processar este evento, desconsiderar
    "SCHOOLCLASS": process_school_class_event, # dim_school_class - ok
    "SCHOOLGRADEGROUP": process_school_grade_group_event, # dim_school_grade_group
    #"SCHOOLIMAGE": process_school_image_event, # nao iremos processar este evento, desconsiderar
    "SCHOOLLEVELSESSION": process_school_level_session_event, # dim_school_level_session
    "SESSION": process_session_event, # dim_session
    "SUBJECTSCHOOLCLASS": process_subject_school_class_event, # dim_section_subject - ok
    "UNOSCHOOL": process_uno_school_event, # dim_school, dim_school_addres, dim_school_phone, dim_school_email - ok
    "USER": process_user_event, # dim_user, dim_user_phone, dim_user_email, dim_user_address - ok
    #"USERARTICLENOTIFICATION": process_user_article_notification_event,  # nao iremos processar este evento, desconsiderar
    #"USERARTICLEROLE": process_user_article_role_event, # nao iremos processar este evento, desconsiderar
    "USERGROUP": process_user_group_event, # dim_user_group
    #"USERIMAGE": process_user_image_event, # nao iremos processar este evento, desconsiderar
    "USERROLE": process_user_role_event, # dim_user_role
    "USERSCHOOLCLASS": process_user_school_class_event # dim_class_participants - ok
}

def process_event_by_verb(payload):
    """
    Processa um evento baseado no seu entityKey.
    
    Args:
        payload (dict): Payload JSON do evento
        
    Returns:
        dict: Dicionário com os registros processados para cada tabela 
              ou None para entityKey desconhecido
    """
    # Extrai o entityKey do payload com segurança
    entity_key = payload.get("meta", {}).get("entityKey")
    
    if not entity_key:
        logger.warning("Campo 'entityKey' não encontrado no payload")
        return None
    
    logger.info(f"Processando evento com entityKey: {entity_key}")
    
    # Busca a função processadora no dicionário de mapeamento
    processor = ENTITY_PROCESSORS.get(entity_key)
    
    if processor:
        return processor(payload)
    else:
        logger.warning(f"Evento com entityKey desconhecido: {entity_key}")
        return None
        
        