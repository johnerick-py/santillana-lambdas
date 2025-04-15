#!/usr/bin/env python
# coding: utf-8

"""
Definições de esquemas e mapeamentos para transformação de dados.
"""

from pyspark.sql.types import (
    StringType,
    BooleanType,
    TimestampType
)

def get_result_scorm_schema():
    """
    Obtém o esquema e mapeamento para a tabela dim_lrs_result_scorm.
    
    Returns:
        tuple: (rename_mapping, expected_schema)
    """
    rename_mapping = {
        "id_sys": "id_system",
        "lrs_id": "lrs_id",
        "client_id": "client_id",
        "id_statement": "statement_id",
        "scorm&46_correctResponsesPattern": "correct_responses_pattern",
        "scorm&46_response": "response",
        "scorm&46_multipleRecording": "multiple_recording",
        "scorm&46_image": "image",
    }

    expected_schema = {
        "dt_load": TimestampType(),
        "id_system": StringType(),
        "lrs_id": StringType(),
        "client_id": StringType(),
        "statement_id": StringType(),
        "correct_responses_pattern": StringType(),
        "response": StringType(),
        "multiple_recording": StringType(),
        "image": StringType()
    }
    
    return rename_mapping, expected_schema

def get_result_schema():
    """
    Obtém o esquema e mapeamento para a tabela dim_lrs_result.
    
    Returns:
        tuple: (rename_mapping, expected_schema)
    """
    rename_mapping = {
        "id_sys": "id_system",
        "lrs_id": "lrs_id",
        "client_id": "client_id",
        "id_statement": "statement_id",
        "success": "success_status",
        "response": "response",
        "duration": "duration",
        "score_scaled": "score_scaled",
        "score_raw": "score_raw",
        "score_min": "score_min",
        "score_max": "score_max",
        "completion": "completion",
        "interactionType": "interaction_type",
        "extensions": "extensions"
    }

    expected_schema = {
        "dt_load": TimestampType(),
        "id_system": StringType(),
        "lrs_id": StringType(),
        "client_id": StringType(),
        "statement_id": StringType(),
        "success_status": BooleanType(),
        "response": StringType(),
        "duration": StringType(),
        "score_scaled": StringType(),
        "score_raw": StringType(),
        "score_min": StringType(),
        "score_max": StringType(),
        "completion": BooleanType(),
        "interaction_type": StringType(),
        "extensions": StringType()
    }
    
    return rename_mapping, expected_schema