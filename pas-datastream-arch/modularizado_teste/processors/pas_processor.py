"""
Módulo que contém os processadores específicos para cada tipo de verbID.
"""
import logging
from utils.redshift_utils import build_processed_record
from models.table_schema import (
    DIM_STATEMENT,
    DIM_VERB,
    LRS_OBJECT_HTTP_COLUMNS,
    LRS_RESULTS_COLUMNS,
    LRS_RESULT_SCORM
)

logger = logging.getLogger()

def process_answered_event(payload):
    """
    Processa eventos com verbID 'answered'.
    """
    processed = {}

    # Processamento para tabela gold.dim_lrs_statement
    processed['gold.dim_lrs_statement'] = build_processed_record(
        [
            (DIM_STATEMENT[1], payload.get("id", None)),  # id_system
            (DIM_STATEMENT[2], payload.get("lrs_id", None)),  # lrs_id
            (DIM_STATEMENT[3], payload.get("client_id", None)),  # client_id
            (DIM_STATEMENT[4], payload.get("statement", {}).get("id", None)),  # statement_id
            (DIM_STATEMENT[5], payload.get("date_insert", None)),  # date_insert
            (DIM_STATEMENT[6], payload.get("active", None)),  # active_status
            (DIM_STATEMENT[7], payload.get("verbID", None)),  # verb_id
            (DIM_STATEMENT[8], payload.get("activityID", None)),  # activity_id
            (
                DIM_STATEMENT[9],
                ",".join(payload.get("relatedAgents", []))
                if isinstance(payload.get("relatedAgents", []), list)
                else payload.get("relatedAgents", None)
            ),  # related_agents
            (
                DIM_STATEMENT[10],
                ",".join(payload.get("relatedActivities", []))
                if isinstance(payload.get("relatedActivities", []), list)
                else payload.get("relatedActivities", None)
            ),  # related_activities
            (DIM_STATEMENT[11], payload.get("statement", {}).get("stored", None)),  # date_stored
            (DIM_STATEMENT[12], payload.get("voided", None)),  # voided_status
            (DIM_STATEMENT[13], payload.get("statement", {}).get("timestamp", None))  # dt_time
        ],
        DIM_STATEMENT
    )

    return processed
