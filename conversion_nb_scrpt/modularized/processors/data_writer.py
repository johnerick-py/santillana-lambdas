import time
import os
import gc
import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hash
from utils.aws_utils import get_aws_secret
from utils.schema_utils import get_result_scorm_schema, get_result_schema
from config.settings import (
    BATCH_SIZE,
    COALESCE_PARTITIONS,
    get_temp_path,
    get_s3_output_path,
    REDSHIFT_HOST,
    REDSHIFT_PORT,
    REDSHIFT_DB,
    REDSHIFT_USER,
    REDSHIFT_PASSWORD,
    REDSHIFT_IAM_ROLE,
    RESULT_TABLE_NAME,
    RESULT_SCORM_TABLE_NAME
)

import uuid
import psutil
import shutil

def clear_local_tmp():
    spark_tmp = "/home/drchapeu/Documents/ajuda_school_level/modularized/tmp/spark-temp"
    if os.path.exists(spark_tmp):
        print("🧹 Limpando diretório temporário local do Spark...")
        shutil.rmtree(spark_tmp)
    os.makedirs(spark_tmp, exist_ok=True)
    print("✅ Diretório /tmp/spark-temp limpo e recriado.")

def print_swap_usage():
    swap = psutil.swap_memory()
    print(f"🔄 Swap em uso: {swap.used / (1024**2):.2f} MB de {swap.total / (1024**2):.2f} MB")

def clear_swap():
    print("⛔ Limpando swap...")
    os.system("sudo /sbin/swapoff -a && sudo /sbin/swapon -a")
    print("✅ Swap reiniciado com sucesso.")
    print_swap_usage()

def save_dataframe_in_batches(
    spark,
    df: DataFrame,
    schema_columns,
    table_name,
    batch_size=BATCH_SIZE,
    coalesce_partitions=COALESCE_PARTITIONS
):
    print(f"Salvando {table_name} em {batch_size} lotes...")
    df_ordered = df.select(*schema_columns).cache()
    s3_path = os.path.join(get_s3_output_path(table_name), "batchs")
    write_start = time.time()
    total_batches = 0

    for i in range(batch_size):
        print(f"Processando batch {i+1}/{batch_size}...")
        df_batch = df_ordered.filter((hash(col("statement_id")) % batch_size) == i)

        has_data = bool(df_batch.take(1))
        if has_data:
            unique_id = str(uuid.uuid4())
            filename = f"part-{i:05d}-{unique_id}.snappy.parquet"
            path_to_write = os.path.join(s3_path, filename)


            print(f"💾 Salvando em: {path_to_write}")
            (
                df_batch
                .repartition(coalesce_partitions)
                .write
                .mode("overwrite")
                .parquet(path_to_write)
            )

            print(f"✅ Arquivo salvo: {path_to_write}")
            total_batches += 1

            df_batch.unpersist(blocking=True)
            spark.catalog.clearCache()
            gc.collect()
            clear_swap()
            print("\n=== SWAP LIMPO ===")
        else:
            print(f"Batch {i+1} vazio. Pulando...")

    df_ordered.unpersist(blocking=True)
    spark.catalog.clearCache()
    gc.collect()

    write_time = time.time() - write_start
    print(f"✅ Total de {total_batches:,} batches salvos em {write_time:.2f} segundos")
    return None, s3_path


def register_table_in_catalog(spark, table_name, location_path):
    try:
        print(f"Registrando tabela {table_name} no catálogo...")
        spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        spark.sql(f"DROP TABLE IF EXISTS gold.{table_name}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold.{table_name}
            USING PARQUET
            LOCATION '{location_path}'
        """)
        print(f"Tabela gold.{table_name} registrada com sucesso")
        return True
    except Exception as e:
        print(f"Erro ao registrar tabela {table_name}: {e}")
        return False

def get_redshift_connection():
    try:
        if REDSHIFT_HOST and REDSHIFT_USER and REDSHIFT_PASSWORD:
            return psycopg2.connect(
                dbname=REDSHIFT_DB,
                host=REDSHIFT_HOST,
                port=REDSHIFT_PORT,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD
            )

        print("Obtendo credenciais Redshift do AWS Secrets Manager...")
        conn_database = get_aws_secret()
        return psycopg2.connect(
            dbname=conn_database['dbname'],
            host=conn_database['host'],
            port=5439,
            user=conn_database['username'],
            password=conn_database['password']
        )
    except Exception as e:
        print(f"Erro ao conectar ao Redshift: {e}")
        raise

def perform_copy_to_redshift(table_name, s3_path, iam_role=REDSHIFT_IAM_ROLE):
    print(f"Copiando {table_name} para o Redshift...")
    try:
        copy_start = time.time()
        conn = get_redshift_connection()
        cur = conn.cursor()
        cur.execute(f"""
            COPY gold.{table_name}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
        """)
        conn.commit()
        cur.close()
        conn.close()
        copy_time = time.time() - copy_start
        print(f"✅ COPY no Redshift concluído em {copy_time:.2f} segundos")
        return True
    except Exception as e:
        print(f"❌ Erro ao copiar para o Redshift: {e}")
        return False


def save_and_register_datasets(spark, df_transformed_result_scorm, df_transformed_result, copy_to_redshift=True):
    success = True
    _, expected_schema_result_scorm = get_result_scorm_schema()
    _, expected_schema_result = get_result_schema()

    print("\nSalvando result_scorm...")
    scorm_ordered_columns = list(expected_schema_result_scorm.keys())
    _, scorm_s3_path = save_dataframe_in_batches(
        spark,
        df_transformed_result_scorm,
        scorm_ordered_columns,
        RESULT_SCORM_TABLE_NAME
    )

    if not register_table_in_catalog(spark, RESULT_SCORM_TABLE_NAME, scorm_s3_path):
        success = False

    print("\nSalvando result...")
    result_ordered_columns = list(expected_schema_result.keys())
    _, result_s3_path = save_dataframe_in_batches(
        spark,
        df_transformed_result,
        result_ordered_columns,
        RESULT_TABLE_NAME
    )

    if not register_table_in_catalog(spark, RESULT_TABLE_NAME, result_s3_path):
        success = False

    if copy_to_redshift and success:
        print("\n=== COPIANDO PARA O REDSHIFT ===")
        if not perform_copy_to_redshift(RESULT_SCORM_TABLE_NAME, scorm_s3_path):
            success = False
        if not perform_copy_to_redshift(RESULT_TABLE_NAME, result_s3_path):
            success = False


    return success
