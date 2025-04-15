#!/usr/bin/env python
# coding: utf-8

"""
Funções para carregamento de dados.
"""

import time
from pyspark.sql.functions import col
from config.settings import get_input_path, PROCESS_896_DATA

def load_parquet_data(spark, path, verbose=False):
    """
    Carrega dados de um arquivo parquet e aplica distinct.
    
    Args:
        spark: Sessão Spark.
        path (str): Caminho do arquivo parquet.
        verbose (bool): Se True, exibe contagem de registros.
        
    Returns:
        DataFrame: DataFrame Spark carregado e com distinct aplicado.
    """
    print(f"Carregando dados de {path}...")
    
    # Garante que o caminho usa s3a://
    if path.startswith("s3://"):
        path = path.replace("s3://", "s3a://")
    
    read_start = time.time()
    try:
        df = spark.read.parquet(path)
        read_time = time.time() - read_start
        print(f"Tempo de leitura: {read_time:.2f} segundos")
        
        if verbose:
            count = df.count()
            print(f"Quantidade de registros: {count}")
        
        # Aplica distinct para remover duplicidades
        distinct_start = time.time()
        df = df.distinct()
        distinct_time = time.time() - distinct_start
        print(f"Tempo de processamento distinct: {distinct_time:.2f} segundos")
        
        return df
    except Exception as e:
        print(f"Erro ao carregar dados de {path}: {e}")
        return None


def process_dataset(spark, id_prefix, years, data_type="results"):
    """
    Processa um conjunto de dados para um ID específico.
    
    Args:
        spark: Sessão Spark.
        id_prefix (str): Prefixo do ID (como '7447', '896', etc).
        years (list): Lista de anos a processar.
        data_type (str): Tipo de dados ('results' ou 'objects').
        
    Returns:
        DataFrame: DataFrame unificado ou None se não houver dados.
    """
    print(f"Processando {data_type} para {id_prefix}...")
    dfs = []
    
    # Carrega dados para cada ano especificado
    for year in years:
        path = get_input_path(id_prefix, year, data_type)
        try:
            df_year = load_parquet_data(spark, path)
            if df_year is not None:
                dfs.append(df_year)
                print(f"Dados de {id_prefix} {year} carregados com sucesso")
        except Exception as e:
            print(f"Erro ao carregar dados de {id_prefix} {year}: {e}")
    
    # Se não houver dados, retorna None
    if not dfs:
        print(f"Nenhum dado carregado para {id_prefix}")
        return None
    
    # União dos DataFrames
    read_start = time.time()
    if len(dfs) == 1:
        result_df = dfs[0]
    else:
        base_df = dfs[0]
        for df in dfs[1:]:
            base_df = base_df.union(df)
        result_df = base_df
    
    union_time = time.time() - read_start
    print(f"Tempo de união para {id_prefix}: {union_time:.2f} segundos")
    
    return result_df


def load_datasets(spark, ids_to_process, years_to_process, all_years):
    """
    Carrega todos os datasets necessários para o processamento.
    
    Args:
        spark: Sessão Spark.
        ids_to_process (list): Lista de IDs a processar.
        years_to_process (list): Lista de anos a processar para a maioria dos IDs.
        all_years (list): Lista completa de anos (usado para ID 896 se aplicável).
        
    Returns:
        tuple: (df_result, df_object) DataFrames unificados.
    """
    # Carregar dados de results
    print("\n=== PROCESSANDO DADOS DE RESULTADOS ===")
    result_dfs = []
    for id_prefix in ids_to_process:
        years = all_years if id_prefix == "896" and PROCESS_896_DATA else years_to_process
        df = process_dataset(spark, id_prefix, years, "results")
        if df is not None:
            result_dfs.append(df)
    
    # União dos resultados
    print("\nUnindo todos os DataFrames de resultados...")
    if result_dfs:
        df_result = result_dfs[0]
        for df in result_dfs[1:]:
            df_result = df_result.union(df)
        print(f"União de resultados concluída.")
    else:
        print("Nenhum dado de resultado encontrado.")
        return None, None
    
    # Carregar dados de objetos
    print("\n=== PROCESSANDO DADOS DE OBJETOS ===")
    object_dfs = []
    for id_prefix in ids_to_process:
        years = all_years if id_prefix == "896" and PROCESS_896_DATA else years_to_process
        df = process_dataset(spark, id_prefix, years, "objects")
        if df is not None:
            object_dfs.append(df)
    
    # União dos objetos
    print("\nUnindo todos os DataFrames de objetos...")
    if object_dfs:
        df_object = object_dfs[0]
        for df in object_dfs[1:]:
            df_object = df_object.union(df)
        print(f"União de objetos concluída.")
    else:
        print("Nenhum dado de objeto encontrado.")
        return df_result, None
    
    return df_result, df_object


def prepare_dataframes(df_result, df_object):
    """
    Prepara os DataFrames para transformação, separando-os em componentes necessários.
    
    Args:
        df_result: DataFrame de resultados.
        df_object: DataFrame de objetos.
        
    Returns:
        tuple: (df_result_scorm, df_result_almost, df_object_filtered) DataFrames preparados.
    """
    # Filtra apenas as colunas necessárias dos objetos
    df_object_filtered = df_object.select(
        "id_statement",
        "interactionType"
    )
    
    # Separar df_result_scorm
    df_result_scorm = df_result.select(
        "id_sys",
        "lrs_id",
        "client_id",
        "id_statement",
        "scorm&46_correctResponsesPattern",
        "scorm&46_response",
        "scorm&46_multipleRecording",
        "scorm&46_image",
    )
    
    # Separar df_result_almost
    df_result_almost = df_result.select(
        "id_sys",
        "lrs_id",
        "client_id",
        "id_statement",
        "success",
        "response",
        "duration",
        "score_scaled",
        "score_raw",
        "score_min",
        "score_max",
        "completion"
    )
    
    return df_result_scorm, df_result_almost, df_object_filtered