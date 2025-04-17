
import os
import time
import traceback
import subprocess
import json
import gc
from dotenv import load_dotenv
import psutil
import shutil

load_dotenv()

PROGRESS_FILE = "progress.json"
SAVE_EVERY = 1
_last_saved_index = 0
RESUME_FROM_LAST_PATH = True  # ✅ nova flag de controle
# Path exato de onde deve retomar, se RESUME_FROM_LAST_PATH for True
LAST_PROCESSED_INPUT_PATH = "s3a://pas-prod-silver/wicco-historical-final/7449/2024/results/part-00000-73308c1a-8500-4881-85fc-420b612827e2-c000.snappy.parquet"


def clear_local_tmp():
    spark_tmp = "/home/drchapeu/Documents/ajuda_school_level/modularized/tmp/spark-temp"
    if os.path.exists(spark_tmp):
        print("🧹 Limpando diretório temporário local do Spark...")
        shutil.rmtree(spark_tmp)
    os.makedirs(spark_tmp, exist_ok=True)
    print("✅ Diretório /tmp/spark-temp limpo e recriado.")

def load_last_processed():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f).get("last_processed_id")
        except json.JSONDecodeError:
            print(f"Aviso: {PROGRESS_FILE} está vazio ou inválido.")
    return None

def save_last_processed_conditionally(current_index, id_):
    global _last_saved_index
    if (current_index - _last_saved_index) >= SAVE_EVERY:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({"last_processed_id": id_}, f)
        _last_saved_index = current_index

def verify_aws_sso_login():
    try:
        profile = os.getenv("AWS_PROFILE", "john-prod")
        print(f"Verificando credenciais para o perfil AWS: {profile}")
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity", "--profile", profile],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print("✅ Login AWS SSO ativo e válido.")
            return True
        else:
            print("❌ Login AWS SSO inválido ou expirado.")
            print(result.stderr)
            print(f"→ Execute: aws sso login --profile {profile}")
            return False
    except Exception as e:
        print(f"Erro ao verificar login SSO: {e}")
        return False
        
def print_swap_usage():
    swap = psutil.swap_memory()
    print(f"🔄 Swap em uso: {swap.used / (1024**2):.2f} MB de {swap.total / (1024**2):.2f} MB")
        
def clear_swap():
    print("⛔ Limpando swap...")
    os.system("sudo /sbin/swapoff -a && sudo /sbin/swapon -a")
    print("✅ Swap reiniciado com sucesso.")
    print_swap_usage()

def append_processed_path(res_path, obj_path):
    with open("/home/drchapeu/Documents/ajuda_school_level/modularized/arquivos_processados.txt", "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - RESULT: {res_path} | OBJECT: {obj_path}\n")

def wait_for_memory_free(min_available_gb=2):
    while True:
        available = psutil.virtual_memory().available / (1024 ** 3)
        if available >= min_available_gb:
            break
        print(f"⏳ Aguardando memória RAM liberar... Disponível: {available:.2f} GB")
        time.sleep(5)

# Verificação de login
if not verify_aws_sso_login():
    exit("❌ Login AWS inválido. Encerrando script.")

# Imports após login
from config.settings import YEARS_DEFAULT, ID_PREFIXES_DEFAULT, PROCESS_896_DATA, COPY_TO_REDSHIFT
from config.spark_config import initialize_spark
from utils.aws_utils import get_sso_credentials
from processors.data_loader import load_datasets_file_by_file, prepare_dataframes
from processors.data_transformer import transform_dataframes
from processors.data_writer import save_and_register_datasets

def main():
    total_start = time.time()

    aws_profile = os.getenv("AWS_PROFILE")
    print(f"Utilizando perfil AWS: {aws_profile}")
    # if not get_sso_credentials(aws_profile):
    #     return 1

    spark = initialize_spark()

    try:
        ids_to_process = ID_PREFIXES_DEFAULT.copy()
        years_to_process = YEARS_DEFAULT.copy()
        last_id = load_last_processed()
        if last_id in ids_to_process:
            ids_to_process = ids_to_process[ids_to_process.index(last_id) + 1:]

        all_years = ["2019", "2020", "2021", "2022", "2023", "2024", "2025"] if PROCESS_896_DATA else years_to_process

        for i, id_prefix in enumerate(ids_to_process):
            print(f"\n=== PROCESSANDO ID: {id_prefix} ===")

            for df_result, df_object in load_datasets_file_by_file(
                                                spark,
                                                id_prefix,
                                                years_to_process,
                                                all_years,
                                                resume_from_last_path=RESUME_FROM_LAST_PATH,
                                                last_processed_input_path=LAST_PROCESSED_INPUT_PATH
                                            ):

                print("\n=== PREPARANDO DATAFRAMES PARA PROCESSAMENTO ===")
                
                if hasattr(df_result, "_jdf") and hasattr(df_object, "_jdf"):
                    try:
                        res_path = df_result._jdf.inputFiles()[0]
                        obj_path = df_object._jdf.inputFiles()[0]
                        append_processed_path(res_path, obj_path)
                    except Exception as e:
                        print(f"⚠️ Não foi possível registrar caminho lido: {e}")
                
                df_result_scorm, df_result_almost, df_object_filtered = prepare_dataframes(df_result, df_object)

                print("Realizando join entre result e object...")
                df_result_joined = df_result_almost.join(df_object_filtered, on="id_statement", how="inner")

                print("\n=== TRANSFORMANDO DATAFRAMES ===")
                df_transformed_result_scorm, df_transformed_result = transform_dataframes(df_result_scorm, df_result_joined)

                print("\n=== SALVANDO DATAFRAMES TRANSFORMADOS ===")
                success = save_and_register_datasets(
                    spark,
                    df_transformed_result_scorm,
                    df_transformed_result,
                    copy_to_redshift=COPY_TO_REDSHIFT
                )

                if not success:
                    print("❌ Erro ao salvar dados. Abortando.")
                    return 1

                save_last_processed_conditionally(i, id_prefix)
                print("\n=== SALVO NO S3 ===")
                clear_swap()
                print("\n=== SWAP LIMPO ===")
                wait_for_memory_free()

        print(f"\n✅ Processamento finalizado em {(time.time() - total_start)/60:.2f} minutos.")
        return 0

    except Exception as e:
        print(f"❌ Erro: {e}")
        traceback.print_exc()
        return 1

    finally:
        if 'spark' in locals():
            spark.stop()
            print("🛑 Sessão Spark encerrada.")

if __name__ == "__main__":
    exit(main())