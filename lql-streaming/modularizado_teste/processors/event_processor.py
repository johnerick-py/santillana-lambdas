"""
Módulo para processamento de eventos baseado no entityKey.
"""
import logging
from processors.pas_processor import (
    process_lql_event,
)

logger = logging.getLogger()

# Dicionário de mapeamento entre entityKey e função processadora correspondente
ENTITY_PROCESSORS = {
    "acceso": process_lql_event,
    "callback" : process_lql_event
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
    type_lql = payload.get("type", "")
    action = payload.get("action", "")
    if not type_lql:
        logger.warning("Campo 'type' não encontrado no payload")
        return None
    
    logger.info(f"Processando evento com type: {type_lql} e action: {action}" )
    
    # Busca a função processadora no dicionário de mapeamento
    processor = ENTITY_PROCESSORS.get(type_lql)
    
    if processor:
        return processor(payload)
    else:
        logger.warning(f"Evento com type desconhecido: {type_lql}")
        return None
        
        