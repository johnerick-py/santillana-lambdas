import json
import base64
import boto3
import psycopg2
import os
import re
from datetime import datetime

def transform_keys(obj):
    """
    Função recursiva para transformar todas as chaves em um objeto JSON:
    - Remove caracteres '_' e '-' das chaves
    - Converte todas as chaves para lowercase
    - Remove aspas simples de valores em 'relatedagents'
    """
    if isinstance(obj, dict):
        new_dict = {}
        for key, value in obj.items():
            # Transformar a chave: remover '_', '-' e converter para lowercase
            new_key = key.replace('_', '').replace('-', '').lower()
            
            # Tratamento especial para o campo relatedAgents/relatedagents
            if new_key == 'relatedagents' and isinstance(value, list):
                new_value = []
                for item in value:
                    if isinstance(item, str):
                        # Remover aspas simples, aspas duplas e aspas escapadas
                        cleaned_item = item.replace("'", "").replace('\\"', '').replace('"', '')
                        new_value.append(cleaned_item)
                    else:
                        new_value.append(transform_keys(item))
                new_dict[new_key] = new_value
            else:
                # Transformar o valor recursivamente se for dict ou list
                new_dict[new_key] = transform_keys(value)
        return new_dict
    elif isinstance(obj, list):
        # Se for uma lista, aplicar a transformação para cada item da lista
        return [transform_keys(item) for item in obj]
    else:
        # Se for um valor primitivo, retornar sem alteração
        return obj

def insert_to_redshift(json_data):
    """
    Insere o JSON processado na tabela lrs_events_all do Redshift
    """
    # Obter credenciais do Redshift das variáveis de ambiente
    dbname = os.environ.get('DBNAME')
    host = os.environ.get('HOST')
    port = os.environ.get('PORT')
    user = os.environ.get('USER')
    password = os.environ.get('PASSWORD')
    
    # Tabela de destino fixa
    table_name = "lrs_events_all"
    
    # Conectar ao Redshift
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password
        )
        
        cursor = conn.cursor()
        
        # Converter JSON para string e escapar caracteres especiais para SQL
        json_string = json.dumps(json_data).replace("'", "''")
        
        # Inserir usando o comando copy para evitar problemas de parsing
        query = f"INSERT INTO stream_raw.{table_name} (event) VALUES(JSON_PARSE('{json_string}'))"
        
        # Executar query
        cursor.execute(query)
        conn.commit()
        
        print(f"Registro inserido com sucesso na tabela stream_raw.{table_name}")
        
    except Exception as e:
        print(f"Erro ao inserir no Redshift: {str(e)}")
        if conn:
            conn.rollback()
        raise e
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def lambda_handler(event, context):
    """
    Função Lambda para processar registros do Kinesis e inseri-los no Redshift
    na tabela stream_raw.lrs_events_all
    """
    print("Evento recebido do Kinesis:", json.dumps(event))
    
    # Verificar se existem registros no evento
    if 'Records' not in event or not event['Records']:
        print("Nenhum registro encontrado no evento")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Nenhum registro para processar'})
        }
    
    # Processar cada registro do Kinesis
    for i, record in enumerate(event['Records']):
        print(f"\n--- Processando registro {i+1} ---")
        
        try:
            # Obter o dado codificado em base64
            encoded_data = record['kinesis']['data']
            
            # Decodificar o payload do Kinesis
            decoded_data = base64.b64decode(encoded_data).decode('utf-8')
            print(f"Dados originais decodificados")
            
            # PRIMEIRO: Converter para JSON
            json_data = json.loads(decoded_data)
            
            # Transformar as chaves do JSON
            transformed_json = transform_keys(json_data)
            
            # Inserir no Redshift na tabela fixa lrs_events_all
            insert_to_redshift(transformed_json)
            
        except Exception as e:
            print(f"Erro ao processar registro {i+1}: {str(e)}")
            print(f"Exception detalhes: {type(e).__name__}: {str(e)}")
            # Imprimir o traceback para debugging
            import traceback
            traceback.print_exc()
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processados {len(event["Records"])} registros com sucesso'
        })
    }