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

    # Processamento para tabela gold.dim_lrs_verb
    processed['gold.dim_lrs_verb'] = build_processed_record(
        [
            (DIM_VERB[1], payload.get("id", None)),  # id_system
            (DIM_VERB[2], payload.get("lrs_id", None)),  # lrs_id
            (DIM_VERB[3], payload.get("client_id", None)),  # client_id
            (DIM_VERB[4], payload.get("statement", {}).get("id", None)),  # statement_id
            (DIM_VERB[5], payload.get("statement", {}).get("verb", {}).get("id", None)),  # verb_id
            (DIM_VERB[10], payload.get("statement", {}).get("verb", {}).get("display", {}).get("und", None))  # display_und
        ],
        DIM_VERB
    )

    # Acessa de forma segura os dados aninhados para extensions
    payload_extensions = payload.get("statement", {}).get("object", {}).get("definition", {}).get("extensions", {}).get("http://act_type", {})

    # Processamento para tabela gold.dim_lrs_object_http
    processed['gold.dim_lrs_object_http'] = build_processed_record(
        [
            (LRS_OBJECT_HTTP_COLUMNS[1], payload.get("id", None)),  # id_system
            (LRS_OBJECT_HTTP_COLUMNS[2], payload.get("lrs_id", None)),  # lrs_id
            (LRS_OBJECT_HTTP_COLUMNS[3], payload.get("client_id", None)),  # client_id
            (LRS_OBJECT_HTTP_COLUMNS[4], payload.get("statement", {}).get("id", None)),  # statement_id
            (LRS_OBJECT_HTTP_COLUMNS[5], payload.get("statement", {}).get("object", {}).get("id", None)),  # object_id
            (LRS_OBJECT_HTTP_COLUMNS[23], payload_extensions.get("resourceRefId", None)),  # resource_ref_id
            (LRS_OBJECT_HTTP_COLUMNS[7], payload_extensions.get("resourceId", None)),  # resource_id
            (LRS_OBJECT_HTTP_COLUMNS[11], payload_extensions.get("studentRefId", None)),  # student_id
            (LRS_OBJECT_HTTP_COLUMNS[13], payload_extensions.get("rolId", None)),  # role_id
            (LRS_OBJECT_HTTP_COLUMNS[31], payload_extensions.get("adventureCode", None)),  # adventure_code
            (LRS_OBJECT_HTTP_COLUMNS[17], payload_extensions.get("sourceRefId", None)),  # source_id
            (LRS_OBJECT_HTTP_COLUMNS[25], payload_extensions.get("learningUnitRefId", None)),  # learning_unit_id
            (LRS_OBJECT_HTTP_COLUMNS[15], payload_extensions.get("sectionSubjectRefId", None)),  # section_subject_id
            (LRS_OBJECT_HTTP_COLUMNS[10], payload_extensions.get("schoolClassId", None)),  # school_class_id
            (LRS_OBJECT_HTTP_COLUMNS[14], payload_extensions.get("subjectGradeRefId", None)),  # subject_grade_id
            (LRS_OBJECT_HTTP_COLUMNS[12], payload_extensions.get("schoolRefId", None)),  # school_id
        ],
        LRS_OBJECT_HTTP_COLUMNS
    )

    # Verifica o tipo de interação
    interaction_type = payload.get("statement", {}) \
                      .get("object", {}) \
                      .get("definition", {}) \
                      .get("interactionType", None)

    # Processamento específico para o tipo de interação
    if interaction_type == "long-fill-in":
        def split_value(value, max_length=64000):
            """Divide valor longo em chunks menores se necessário."""
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
                (LRS_RESULT_SCORM[1], payload.get("id", None)),  # id_system
                (LRS_RESULT_SCORM[2], payload.get("lrs_id", None)),  # lrs_id
                (LRS_RESULT_SCORM[3], payload.get("client_id", None)),  # client_id
                (LRS_RESULT_SCORM[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (LRS_RESULT_SCORM[5], payload.get("statement", {})
                 .get("result", {})
                 .get("extensions", {})
                 .get("http://scorm&46;com/extensions/usa-data", {})
                 .get("correctResponsesPattern", None)),  # correct_responses_pattern
                (LRS_RESULT_SCORM[6], response_value_split),  # response
                (LRS_RESULT_SCORM[7], payload.get("statement", {})
                 .get("result", {})
                 .get("extensions", {})
                 .get("http://scorm&46;com/extensions/usa-data", {})
                 .get("multipleRecording", None)),  # multiple_recording
            ],
            LRS_RESULT_SCORM
        )
        logger.info("Processado evento 'answered' com resposta aberta")
    elif interaction_type == "matching":
        processed['gold.dim_lrs_result_scorm'] = build_processed_record(
            [
                (LRS_RESULT_SCORM[1], payload.get("id", None)),  # id_system
                (LRS_RESULT_SCORM[2], payload.get("lrs_id", None)),  # lrs_id
                (LRS_RESULT_SCORM[3], payload.get("client_id", None)),  # client_id
                (LRS_RESULT_SCORM[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (LRS_RESULT_SCORM[5], payload.get("statement", {}).get("result", {}).get("extensions", {}).get("http://scorm&46;com/extensions/usa-data", {}).get("correctResponsesPattern", None)),  # correct_responses_pattern
                (LRS_RESULT_SCORM[6], payload.get("statement", {}).get("result", {}).get("extensions", {}).get("http://scorm&46;com/extensions/usa-data", {}).get("response", None)),  # response
                (LRS_RESULT_SCORM[7], payload.get("statement", {}).get("result", {}).get("extensions", {}).get("http://scorm&46;com/extensions/usa-data", {}).get("multipleRecording", None)),  # multiple_recording
            ],
            LRS_RESULT_SCORM
        )
        logger.info("Processado evento 'answered' com correspondência")
    else:
        logger.warning(f"Tipo de interação não reconhecido para 'answered': {interaction_type}")

    return processed

def process_experienced_event(payload):
    """
    Processa eventos com verbID 'experienced'.
    """
    processed = {}
    
    # Verifica se extensions contém "http://ids"
    definition = payload.get("statement", {}).get("object", {}).get("definition", {})
    extensions = definition.get("extensions", {})

    if "http://ids" in extensions:
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

        # Acessa de forma segura os dados aninhados usando .get
        payload_ids = payload.get("statement", {}).get("object", {}).get("definition", {}).get("extensions", {}).get("http://ids", {})

        processed['gold.dim_lrs_object_http'] = build_processed_record(
            [
                (LRS_OBJECT_HTTP_COLUMNS[1], payload.get("id", None)),  # id_system
                (LRS_OBJECT_HTTP_COLUMNS[2], payload.get("lrs_id", None)),  # lrs_id
                (LRS_OBJECT_HTTP_COLUMNS[3], payload.get("client_id", None)),  # client_id
                (LRS_OBJECT_HTTP_COLUMNS[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (LRS_OBJECT_HTTP_COLUMNS[5], payload.get("statement", {}).get("object", {}).get("id", None)),  # object_id
                (LRS_OBJECT_HTTP_COLUMNS[23], payload_ids.get("resourceRefId", None)),  # resource_ref_id
                (LRS_OBJECT_HTTP_COLUMNS[7], payload_ids.get("resourceId", None)),  # resource_id
                (LRS_OBJECT_HTTP_COLUMNS[11], payload_ids.get("studentRefId", None)),  # student_id
                (LRS_OBJECT_HTTP_COLUMNS[13], payload_ids.get("rolId", None)),  # role_id
                (LRS_OBJECT_HTTP_COLUMNS[31], payload_ids.get("adventureCode", None)),  # adventure_code
                (LRS_OBJECT_HTTP_COLUMNS[17], payload_ids.get("sourceRefId", None)),  # source_id
                (LRS_OBJECT_HTTP_COLUMNS[25], payload_ids.get("learningUnitRefId", None)),  # learning_unit_id
                (LRS_OBJECT_HTTP_COLUMNS[15], payload_ids.get("sectionSubjectRefId", None)),  # section_subject_id
                (LRS_OBJECT_HTTP_COLUMNS[10], payload_ids.get("schoolClassId", None)),  # school_class_id
                (LRS_OBJECT_HTTP_COLUMNS[14], payload_ids.get("subjectGradeRefId", None)),  # subject_grade_id
                (LRS_OBJECT_HTTP_COLUMNS[12], payload_ids.get("schoolRefId", None)),  # school_id
            ],
            LRS_OBJECT_HTTP_COLUMNS
        )

        processed['gold.dim_lrs_verb'] = build_processed_record(
            [
                (DIM_VERB[1], payload.get("id", None)),  # id_system
                (DIM_VERB[2], payload.get("lrs_id", None)),  # lrs_id
                (DIM_VERB[3], payload.get("client_id", None)),  # client_id
                (DIM_VERB[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (DIM_VERB[5], payload.get("statement", {}).get("verb", {}).get("id", None)),  # verb_id
                (DIM_VERB[10], payload.get("statement", {}).get("verb", {}).get("display", {}).get("und", None))  # display_und
            ],
            DIM_VERB
        )
    else:
        logger.info("Evento 'experienced' sem extensões 'http://ids'")

    return processed

def process_attempted_event(payload):
    """
    Processa eventos com verbID 'attempted'.
    """
    # Implementação simplificada
    processed = {}
    logger.info("Processando evento 'attempted'")
    return processed

def process_terminated_event(payload):
    """
    Processa eventos com verbID 'terminated'.
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

    # Processamento para tabela gold.dim_lrs_verb
    processed['gold.dim_lrs_verb'] = build_processed_record(
        [
            (DIM_VERB[1], payload.get("id", None)),  # id_system
            (DIM_VERB[2], payload.get("lrs_id", None)),  # lrs_id
            (DIM_VERB[3], payload.get("client_id", None)),  # client_id
            (DIM_VERB[4], payload.get("statement", {}).get("id", None)),  # statement_id
            (DIM_VERB[5], payload.get("statement", {}).get("verb", {}).get("id", None)),  # verb_id
            (DIM_VERB[10], payload.get("statement", {}).get("verb", {}).get("display", {}).get("und", None))  # display_und
        ],
        DIM_VERB
    )

    # Acessa dados aninhados
    payload_ids = payload.get("statement", {}).get("object", {}).get("definition", {}).get("extensions", {}).get("http://ids", {})

    # Processamento para tabela gold.dim_lrs_object_http
    processed['gold.dim_lrs_object_http'] = build_processed_record(
        [
            (LRS_OBJECT_HTTP_COLUMNS[1], payload.get("id", None)),  # id_system
            (LRS_OBJECT_HTTP_COLUMNS[2], payload.get("lrs_id", None)),  # lrs_id
            (LRS_OBJECT_HTTP_COLUMNS[3], payload.get("client_id", None)),  # client_id
            (LRS_OBJECT_HTTP_COLUMNS[4], payload.get("statement", {}).get("id", None)),  # statement_id
            (LRS_OBJECT_HTTP_COLUMNS[5], payload.get("statement", {}).get("object", {}).get("id", None)),  # object_id
            (LRS_OBJECT_HTTP_COLUMNS[23], payload_ids.get("resourceRefId", None)),  # resource_ref_id
            (LRS_OBJECT_HTTP_COLUMNS[7], payload_ids.get("resourceId", None)),  # resource_id
            (LRS_OBJECT_HTTP_COLUMNS[11], payload_ids.get("studentRefId", None)),  # student_id
            (LRS_OBJECT_HTTP_COLUMNS[13], payload_ids.get("rolId", None)),  # role_id
            (LRS_OBJECT_HTTP_COLUMNS[31], payload_ids.get("adventureCode", None)),  # adventure_code
            (LRS_OBJECT_HTTP_COLUMNS[17], payload_ids.get("sourceRefId", None)),  # source_id
            (LRS_OBJECT_HTTP_COLUMNS[25], payload_ids.get("learningUnitRefId", None)),  # learning_unit_id
            (LRS_OBJECT_HTTP_COLUMNS[15], payload_ids.get("sectionSubjectRefId", None)),  # section_subject_id
            (LRS_OBJECT_HTTP_COLUMNS[10], payload_ids.get("schoolClassId", None)),  # school_class_id
            (LRS_OBJECT_HTTP_COLUMNS[14], payload_ids.get("subjectGradeRefId", None)),  # subject_grade_id
            (LRS_OBJECT_HTTP_COLUMNS[12], payload_ids.get("schoolRefId", None)),  # school_id
        ], 
        LRS_OBJECT_HTTP_COLUMNS
    )

    # Processamento para tabela gold.dim_lrs_result se existir
    if payload.get("statement", {}).get("result") is not None:
        processed['gold.dim_lrs_result'] = build_processed_record(
            [
                (LRS_RESULTS_COLUMNS[1], payload.get("id", None)),  # id_system
                (LRS_RESULTS_COLUMNS[2], payload.get("lrs_id", None)),  # lrs_id
                (LRS_RESULTS_COLUMNS[3], payload.get("client_id", None)),  # client_id
                (LRS_RESULTS_COLUMNS[4], payload.get("statement", {}).get("id", None)),  # statement_id
                (LRS_RESULTS_COLUMNS[5], payload.get("statement", {}).get("result", {}).get("completion", None)),  # success_status
                (LRS_RESULTS_COLUMNS[7], payload.get("statement", {}).get("result", {}).get("duration", None)),  # duration
                (LRS_RESULTS_COLUMNS[8], payload.get("statement", {}).get("result", {}).get("score", {}).get("scaled", None)),  # score_scaled
                (LRS_RESULTS_COLUMNS[9], payload.get("statement", {}).get("result", {}).get("score", {}).get("raw", None)),  # score_raw
                (LRS_RESULTS_COLUMNS[10], payload.get("statement", {}).get("result", {}).get("score", {}).get("min", None)),  # score_min
                (LRS_RESULTS_COLUMNS[11], payload.get("statement", {}).get("result", {}).get("score", {}).get("max", None)),  # score_max
                (LRS_RESULTS_COLUMNS[12], payload.get("statement", {}).get("result", {}).get("completion", None)),  # completion
                (LRS_RESULTS_COLUMNS[13], payload.get("statement", {}).get("object", {}).get("definition", {}).get("interactionType", None)),  # interaction_type
                (LRS_RESULTS_COLUMNS[14], 'http://scorm&46;com/extensions/usa-data')  # extensions
            ],
            LRS_RESULTS_COLUMNS
        )

    logger.info("Processado evento 'terminated'")
    return processed