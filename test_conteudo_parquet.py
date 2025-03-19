# import pandas as pd

# def visualizar_parquet(caminho_arquivo, linhas=5):
#     """
#     Lê e exibe as primeiras linhas de um arquivo Parquet.
    
#     :param caminho_arquivo: Caminho do arquivo Parquet
#     :param linhas: Número de linhas a serem exibidas (padrão: 5)
#     """
#     try:
#         df = pd.read_parquet(caminho_arquivo)
#         print(df.head(linhas))
#     except Exception as e:
#         print(f"Erro ao ler o arquivo Parquet: {e}")

# # Exemplo de uso
# caminho_do_arquivo = "part-00000-00d8fcf9-106e-4b96-9d78-46b4d1982b0a-c000.snappy.parquet"  # Substitua pelo caminho real
# display_linhas = 10  # Número de linhas que deseja visualizar
# visualizar_parquet(caminho_do_arquivo, display_linhas)

import pyarrow.parquet as pq

caminho_do_arquivo = "part-00000-16adff8f-b5c0-4fa4-aab4-4446629a6586-c000.snappy.parquet" 
table = pq.read_table(caminho_do_arquivo)
print(table)