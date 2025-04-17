import time
import re
import gc
import os
import boto3
import psutil
from pyspark.sql.functions import col
from config.settings import get_input_path, PROCESS_896_DATA

def list_parquet_paths_with_size(prefix):
    s3 = boto3.client("s3")
    match = re.match(r"s3a://([^/]+)/(.+)", prefix)
    if not match:
        return []
    bucket, prefix = match.groups()
    paginator = s3.get_paginator("list_objects_v2")
    result = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                result.append((f"s3a://{bucket}/{obj['Key']}", obj["Size"]))
    return result

def print_swap_usage():
    swap = psutil.swap_memory()
    print(f"🔄 Swap em uso: {swap.used / (1024**2):.2f} MB de {swap.total / (1024**2):.2f} MB")

def clear_swap():
    print("⛔ Limpando swap...")
    os.system("sudo /sbin/swapoff -a && sudo /sbin/swapon -a")
    print("✅ Swap reiniciado com sucesso.")

def load_parquet_data(spark, path, verbose=False):
    print(f"Carregando dados de {path}...")
    if path.startswith("s3://"):
        path = path.replace("s3://", "s3a://")

    read_start = time.time()
    try:
        df = spark.read.parquet(path).repartition(10)
        print("=== Plano de execução para leitura parquet ===")
        df.explain(True)
        read_time = time.time() - read_start
        print(f"Tempo de leitura: {read_time:.2f} segundos")

        if verbose:
            try:
                has_data = not df.rdd.isEmpty()
            except Exception:
                has_data = df.count() > 0
            if has_data:
                print("O DataFrame contém registros.")
            else:
                print("O DataFrame está vazio.")

        distinct_start = time.time()
        print("=== Plano de execução para distinct ===")
        df = df.distinct()
        df.explain(True)
        distinct_time = time.time() - distinct_start
        print(f"Tempo de processamento distinct: {distinct_time:.2f} segundos")

        return df
    except Exception as e:
        print(f"Erro ao carregar dados de {path}: {e}")
        return None

def load_datasets_file_by_file(spark, id_prefix, years, all_years, resume_from_last_path=True, last_processed_input_path=None):
    def get_paths_for_type(data_type):
        all_paths = []
        for year in (all_years if id_prefix == "896" and PROCESS_896_DATA else years):
            path_prefix = get_input_path(id_prefix, year, data_type)
            all_paths.extend(list_parquet_paths_with_size(path_prefix))
        return sorted(all_paths, key=lambda x: x[0])  # ordenado por nome de path

    print("\n=== INICIANDO CARREGAMENTO DE UM ARQUIVO POR VEZ ===")
    result_paths = get_paths_for_type("results")
    object_paths = get_paths_for_type("objects")

    # Retomar do último arquivo processado
    if resume_from_last_path and last_processed_input_path:
        try:
            last_idx = next(i for i, (res, _) in enumerate(result_paths) if res == last_processed_input_path)
            result_paths = result_paths[last_idx:]
            object_paths = object_paths[last_idx:]
            print(f"🔁 Retomando a partir de: {last_processed_input_path}")
        except StopIteration:
            print(f"⚠️ Caminho {last_processed_input_path} não encontrado. Iniciando do começo.")

    for (res_path, _), (obj_path, _) in zip(result_paths, object_paths):
        print(f"\n🔹 Processando arquivos:\nResult: {res_path}\nObject: {obj_path}")
        df_result = load_parquet_data(spark, res_path).distinct()
        df_object = load_parquet_data(spark, obj_path).distinct()

        yield df_result, df_object

        with open("/home/drchapeu/Documents/ajuda_school_level/modularized/arquivos_processados.txt", "a") as f:
            f.write(f"{res_path}\n")
            f.write(f"{obj_path}\n")

        df_result.unpersist(blocking=True)
        df_object.unpersist(blocking=True)
        spark.catalog.clearCache()
        gc.collect()
        clear_swap()


    print("\n=== INICIANDO CARREGAMENTO DE UM ARQUIVO POR VEZ ===")
    result_paths = get_paths_for_type("results")
    object_paths = get_paths_for_type("objects")

    for (res_path, _), (obj_path, _) in zip(result_paths, object_paths):
        print(f"\n🔹 Processando arquivos:\nResult: {res_path}\nObject: {obj_path}")
        df_result = load_parquet_data(spark, res_path).distinct()
        df_object = load_parquet_data(spark, obj_path).distinct()

        yield df_result, df_object

        df_result.unpersist(blocking=True)
        df_object.unpersist(blocking=True)
        spark.catalog.clearCache()
        gc.collect()
        clear_swap()

def prepare_dataframes(df_result, df_object):
    df_object_filtered = df_object.select(
        "id_statement",
        "interactionType"
    )

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
