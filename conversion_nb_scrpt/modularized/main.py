#!/usr/bin/env python
# coding: utf-8

"""
Script principal para processamento dos dados WICCO para Gold.
Este script orquestra o fluxo de trabalho completo para transformação de dados.
"""

import os
import time
import traceback
from dotenv import load_dotenv

# Carregar configurações do arquivo .env
load_dotenv()

# Importar módulos do projeto
from config.settings import YEARS_DEFAULT, ID_PREFIXES_DEFAULT, PROCESS_896_DATA, COPY_TO_REDSHIFT
from config.spark_config import initialize_spark
from utils.aws_utils import get_sso_credentials
from processors.data_loader import load_datasets, prepare_dataframes
from processors.data_transformer import transform_dataframes
from processors.data_writer import save_and_register_datasets

def main():
    """Função principal que orquestra todo o processamento"""
    # Marca o tempo total
    total_start = time.time()
    
    # Obtém o perfil AWS a partir das variáveis de ambiente
    aws_profile = os.getenv("AWS_PROFILE")
    print(f"Utilizando perfil AWS: {aws_profile}")
    
    # Passo 1: Obter credenciais AWS via SSO
    if not get_sso_credentials(aws_profile):
        print("Falha ao obter credenciais AWS. Encerrando...")
        return 1
    
    # Passo 2: Inicializar Spark
    spark = initialize_spark()
    
    try:
        # Passo 3: Definir anos e IDs a processar
        ids_to_process = ID_PREFIXES_DEFAULT.copy()
        years_to_process = YEARS_DEFAULT.copy()
        
        # Se deve processar dados do ID 896 (que tem anos anteriores)
        if PROCESS_896_DATA:
            all_years = ["2019", "2020", "2021", "2022", "2023", "2024", "2025"]
            print(f"Configurado para processar também ID 896 com anos: {all_years}")
        else:
            all_years = years_to_process
        
        # Passo 4: Carregar todos os datasets
        print("\n=== CARREGANDO DATASETS ===")
        df_result, df_object = load_datasets(spark, ids_to_process, years_to_process, all_years)
        
        if df_result is None or df_object is None:
            print("Falha ao carregar datasets necessários. Encerrando...")
            return 1
        
        # Passo 5: Preparar DataFrames para processamento
        print("\n=== PREPARANDO DATAFRAMES PARA PROCESSAMENTO ===")
        df_result_scorm, df_result_almost, df_object_filtered = prepare_dataframes(df_result, df_object)
        
        # Join do resultado com objeto
        print("Realizando join entre result e object...")
        df_result_joined = df_result_almost.join(df_object_filtered, on="id_statement", how="inner")
        
        # Passo 6: Transformar DataFrames
        print("\n=== TRANSFORMANDO DATAFRAMES ===")
        df_transformed_result_scorm, df_transformed_result = transform_dataframes(
            df_result_scorm, df_result_joined
        )
        
        # Passo 7: Salvar DataFrames transformados e registrar no catálogo
        print("\n=== SALVANDO DATAFRAMES TRANSFORMADOS ===")
        success = save_and_register_datasets(
            spark, 
            df_transformed_result_scorm, 
            df_transformed_result, 
            copy_to_redshift=COPY_TO_REDSHIFT
        )
        
        if not success:
            print("Ocorreram erros durante o salvamento de dados. Verifique os logs.")
            return 1
        
        # Calcula tempo total de execução
        total_time = time.time() - total_start
        print(f"\nProcessamento concluído com sucesso!")
        print(f"Tempo total: {total_time:.2f} segundos ({total_time/60:.2f} minutos)")
        
        return 0
        
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        traceback.print_exc()
        return 1
    
    finally:
        # Encerra a sessão Spark
        if 'spark' in locals():
            spark.stop()
            print("Sessão Spark encerrada.")


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)