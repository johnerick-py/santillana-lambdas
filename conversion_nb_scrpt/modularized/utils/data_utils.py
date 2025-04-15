#!/usr/bin/env python
# coding: utf-8

"""
Utilitários para manipulação e transformação de dados.
"""

from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from config.settings import EXCLUDED_STATEMENT_IDS

def truncate_to_bytes(s, max_bytes=256):
    """
    Trunca a string com base no tamanho em bytes (UTF-8).
    
    Args:
        s: String a ser truncada.
        max_bytes (int): Tamanho máximo em bytes.
        
    Returns:
        str: String truncada.
    """
    if s is None:
        return None
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    # Realiza busca binária para encontrar o ponto de corte
    lo, hi = 0, len(s)
    while lo < hi:
        mid = (lo + hi) // 2
        if len(s[:mid].encode("utf-8")) <= max_bytes:
            lo = mid + 1
        else:
            hi = mid
    # lo-1 é o maior índice que satisfaz a condição
    return s[:lo-1]

# UDF para truncamento de strings com base no tamanho em bytes
truncate_to_bytes_udf = udf(truncate_to_bytes, StringType())

def filter_invalid_statements(df):
    """
    Filtra statement_ids nulos ou inválidos de um DataFrame.
    
    Args:
        df: DataFrame Spark a ser filtrado.
        
    Returns:
        DataFrame: DataFrame filtrado.
    """
    filtered_df = df.filter(col('statement_id').isNotNull())
    filtered_df = filtered_df.filter(~col('statement_id').isin(EXCLUDED_STATEMENT_IDS))
    return filtered_df

def apply_truncate_to_columns(df, columns):
    """
    Aplica a função de truncamento em colunas específicas.
    
    Args:
        df: DataFrame Spark.
        columns (list): Lista de nomes de colunas para truncar.
        
    Returns:
        DataFrame: DataFrame com colunas truncadas.
    """
    result_df = df
    for column in columns:
        if column in df.columns:
            result_df = result_df.withColumn(
                column, 
                truncate_to_bytes_udf(col(column))
            )
    return result_df