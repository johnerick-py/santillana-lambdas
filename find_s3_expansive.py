import boto3
import json
import os
import tempfile
import concurrent.futures
from operator import itemgetter
import threading

def count_json_lines(s3_client, bucket_name, object_key):
    """
    Faz o download de um arquivo JSON do S3 e conta o número de linhas.
    Funciona tanto para JSONs de linha única quanto para JSONLines.
    """
    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
        temp_filename = temp_file.name
    
    try:
        # Faz o download do arquivo para um arquivo temporário
        s3_client.download_file(bucket_name, object_key, temp_filename)
        
        # Verifica se é um JSON válido
        try:
            # Tenta abrir como JSON normal
            with open(temp_filename, 'r') as f:
                data = json.load(f)
                
            # Se for um objeto ou array JSON normal, conta quantos itens ele tem
            # (se for array de alto nível) ou retorna 1 (se for um objeto)
            if isinstance(data, list):
                return len(data)
            else:
                return 1
                
        except json.JSONDecodeError:
            # Se falhar, tenta tratar como JSONLines (um objeto JSON por linha)
            line_count = 0
            with open(temp_filename, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:  # Pula linhas vazias
                        try:
                            json.loads(line)
                            line_count += 1
                        except json.JSONDecodeError:
                            pass  # Ignora linhas que não são JSON válido
            
            return line_count
            
    finally:
        # Limpa o arquivo temporário
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def find_json_with_most_lines(profile_name, bucket_name, prefix='', max_workers=20):
    """
    Encontra o arquivo JSON no bucket S3 com o maior número de linhas,
    utilizando multithreading para processar vários arquivos simultaneamente.
    
    Args:
        profile_name: Nome do perfil AWS para autenticação
        bucket_name: Nome do bucket S3
        prefix: Prefixo opcional para filtrar objetos
        max_workers: Número máximo de threads para processamento paralelo
        
    Returns:
        Um dicionário com o nome do arquivo e o número de linhas
    """
    # Cria uma sessão usando o perfil especificado
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client('s3')
    
    # Lista todos os objetos no bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    json_files = []
    
    # Filtra para encontrar apenas arquivos JSON
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.lower().endswith('.json'):
                    json_files.append(key)
    
    if not json_files:
        return {"error": "Nenhum arquivo JSON encontrado no bucket"}
    
    print(f"Encontrados {len(json_files)} arquivos JSON. Analisando em paralelo...")
    
    # Contador para acompanhar o progresso
    counter = {"processed": 0, "total": len(json_files)}
    counter_lock = threading.Lock()
    
    def process_file(json_file):
        # Cada thread obtém sua própria conexão S3
        thread_session = boto3.Session(profile_name=profile_name)
        thread_s3_client = thread_session.client('s3')
        
        line_count = count_json_lines(thread_s3_client, bucket_name, json_file)
        
        # Atualiza o contador de progresso
        with counter_lock:
            counter["processed"] += 1
            current = counter["processed"]
            total = counter["total"]
            if current % 10 == 0 or current == total:  # Relata a cada 10 arquivos ou no final
                print(f"Progresso: {current}/{total} arquivos processados ({(current/total)*100:.1f}%)")
        
        return {"arquivo": json_file, "linhas": line_count}
    
    # Processa arquivos em paralelo usando ThreadPoolExecutor
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submete todos os arquivos para processamento
        future_to_file = {executor.submit(process_file, json_file): json_file for json_file in json_files}
        
        # Coleta resultados à medida que são concluídos
        for future in concurrent.futures.as_completed(future_to_file):
            file = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                print(f"Arquivo {file} gerou exceção: {exc}")
    
    # Ordena os resultados por número de linhas (decrescente)
    results.sort(key=itemgetter("linhas"), reverse=True)
    
    return results[0] if results else {"error": "Falha ao processar arquivos JSON"}

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Encontrar o arquivo JSON com mais linhas em um bucket S3")
    parser.add_argument("--bucket", default="pas-prod-landing-zone", help="Nome do bucket S3 (default: 'pas-prod-landing-zone')")
    parser.add_argument("--profile", default="john-prod", help="Nome do perfil AWS (default: 'john-prod')")
    parser.add_argument("--prefix", default="selected/sms_filtered/sms_USER/", help="Prefixo opcional para filtrar objetos do S3")
    parser.add_argument("--threads", type=int, default=20, help="Número de threads para processamento paralelo (default: 20)")
    
    args = parser.parse_args()
    
    resultado = find_json_with_most_lines(args.profile, args.bucket, args.prefix, args.threads)
    
    if "error" in resultado:
        print(f"Erro: {resultado['error']}")
    else:
        print(f"\nArquivo com mais linhas: {resultado['arquivo']}")
        print(f"Número de linhas: {resultado['linhas']}")