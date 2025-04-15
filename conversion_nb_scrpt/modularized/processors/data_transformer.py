#!/usr/bin/env python
# coding: utf-8

"""
Funções para transformação de dados.
"""

from pyspark.sql.functions import col, lit, current_timestamp
from utils.schema_utils import get_result_scorm_schema, get_result_schema
from utils.data_utils import apply_truncate_to_columns, filter_invalid_statements
from config.settings import TRUNCATE_COLUMNS


def transform_dataframe(df, rename_mapping, expected_schema, truncate_columns=None):
    """
    Transforma um DataFrame de acordo com o esquema esperado.
    
    Args:
        df: DataFrame Spark a ser transformado.
        rename_mapping (dict): Mapeamento de renomeação de colunas.
        expected_schema (dict): Esquema esperado com tipos de dados.
        truncate_columns (list): Colunas para truncar.
        
    Returns:
        DataFrame: DataFrame transformado.
    """
    # Seleciona e renomeia as colunas presentes no mapeamento
    transformed_df = (
        df.select(
            *[
                col(old).cast(expected_schema[new]).alias(new)
                for old, new in rename_mapping.items()
                if old in df.columns
            ]
        )
        .withColumn("dt_load", current_timestamp())
    )
    
    # Adiciona colunas que não existem no mapeamento, mas estão no esquema esperado
    missing_cols = set(expected_schema.keys()) - set(transformed_df.columns)
    for missing_col in missing_cols:
        transformed_df = transformed_df.withColumn(
            missing_col,
            lit(None).cast(expected_schema[missing_col])
        )
    
    # Trunca colunas específicas se necessário
    if truncate_columns:
        transformed_df = apply_truncate_to_columns(transformed_df, truncate_columns)
    
    # Filtra valores inválidos
    transformed_df = filter_invalid_statements(transformed_df)
    
    return transformed_df


def transform_dataframes(df_result_scorm, df_result_joined):
    """
    Transforma os DataFrames de acordo com os esquemas esperados.
    
    Args:
        df_result_scorm: DataFrame de resultado SCORM.
        df_result_joined: DataFrame de resultado unido com objeto.
        
    Returns:
        tuple: (df_transformed_result_scorm, df_transformed_result) DataFrames transformados.
    """
    # Obter esquemas e mapeamentos
    rename_mapping_result_scorm, expected_schema_result_scorm = get_result_scorm_schema()
    rename_mapping_result, expected_schema_result = get_result_schema()
    
    # Transformar result_scorm
    print("Transformando result_scorm...")
    df_transformed_result_scorm = transform_dataframe(
        df_result_scorm, 
        rename_mapping_result_scorm, 
        expected_schema_result_scorm,
        TRUNCATE_COLUMNS
    )
    
    # Transformar result
    print("Transformando result...")
    df_transformed_result = transform_dataframe(
        df_result_joined, 
        rename_mapping_result, 
        expected_schema_result,
        TRUNCATE_COLUMNS
    )
    
    return df_transformed_result_scorm, df_transformed_result