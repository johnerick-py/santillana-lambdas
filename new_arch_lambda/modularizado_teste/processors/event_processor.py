"""
Módulo para processamento de eventos baseado no verbID.
"""
import logging
from processors.verb_processor import (
    process_answered_event,
    process_experienced_event,
    process_attempted_event,
    process_terminated_event
)

logger = logging.getLogger()

def process_event_by_verb(payload):
    """
    Processa um evento baseado no seu verbID.
    
    Args:
        payload (dict): Payload JSON do evento
        
    Returns:
        dict: Dicionário com os registros processados para cada tabela 
              ou None para verbID desconhecido
    """
    verb_id = payload.get("verbID")
    
    logger.info(f"Processando evento com verbID: {verb_id}")
    
    if verb_id == "http://adlnet.gov/expapi/verbs/answered":
        return process_answered_event(payload)
    elif verb_id == "http://adlnet.gov/expapi/verbs/experienced":
        return process_experienced_event(payload)
    elif verb_id == "http://adlnet.gov/expapi/verbs/attempted":
        return process_attempted_event(payload)
    elif verb_id == "http://adlnet.gov/expapi/verbs/terminated":
        return process_terminated_event(payload)
    else:
        # VerbID desconhecido
        logger.warning(f"VerbID desconhecido: {verb_id}")
        return None