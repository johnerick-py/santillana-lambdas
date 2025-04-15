#!/usr/bin/env python
# coding: utf-8

"""
Utilitários para interação com serviços AWS.
"""

import os
import json
import boto3
import subprocess
from config.settings import AWS_REGION, SECRET_NAME

def get_sso_credentials(profile_name):
    """
    Obtém credenciais AWS através do SSO e configura variáveis de ambiente.
    
    Args:
        profile_name (str): Nome do perfil AWS configurado.
        
    Returns:
        bool: True se as credenciais foram obtidas com sucesso, False caso contrário.
    """
    try:
        # Verificar se as variáveis já existem
        if os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'):
            print("Variáveis de ambiente AWS já estão configuradas.")
            print(f"  AWS_ACCESS_KEY_ID: {os.environ.get('AWS_ACCESS_KEY_ID')[:4]}****")
            print(f"  AWS_SECRET_ACCESS_KEY: {os.environ.get('AWS_SECRET_ACCESS_KEY')[:4]}****")
            if os.environ.get('AWS_SESSION_TOKEN'):
                print(f"  AWS_SESSION_TOKEN: Configurado")
            return True
            
        # Primeiro, tente obter via AWS CLI (método mais confiável)
        try:
            # Execute o comando aws sts get-caller-identity
            result = subprocess.run(
                ["aws", "sts", "get-caller-identity", "--profile", profile_name, "--output", "json"],
                capture_output=True, text=True, check=True
            )
            identity = json.loads(result.stdout)
            print(f"Conectado como: {identity.get('Arn', 'Desconhecido')}")
            
            # Execute o comando aws sts get-session-token
            result = subprocess.run(
                ["aws", "sts", "get-session-token", "--profile", profile_name, "--output", "json"],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                session_data = json.loads(result.stdout)
                credentials = session_data.get('Credentials', {})
                
                if credentials:
                    os.environ['AWS_ACCESS_KEY_ID'] = credentials.get('AccessKeyId')
                    os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.get('SecretAccessKey')
                    os.environ['AWS_SESSION_TOKEN'] = credentials.get('SessionToken')
                    
                    # Configure also Hadoop system properties for S3A
                    os.environ['HADOOP_ACCESS_KEY'] = credentials.get('AccessKeyId')
                    os.environ['HADOOP_SECRET_KEY'] = credentials.get('SecretAccessKey')
                    os.environ['HADOOP_SESSION_TOKEN'] = credentials.get('SessionToken')
                    
                    # Configure AWS system properties (alternative approach)
                    os.environ['aws.accessKeyId'] = credentials.get('AccessKeyId')
                    os.environ['aws.secretKey'] = credentials.get('SecretAccessKey') 
                    
                    print(f"Variáveis de ambiente configuradas via AWS CLI get-session-token.")
                    print(f"  AWS_ACCESS_KEY_ID: {credentials.get('AccessKeyId')[:4]}****")
                    print(f"  AWS_SECRET_ACCESS_KEY: {credentials.get('SecretAccessKey')[:4]}****")
                    print(f"  AWS_SESSION_TOKEN: Configurado")
                    
                    # Configurar boto3 com as credenciais obtidas
                    boto3.setup_default_session(
                        aws_access_key_id=credentials.get('AccessKeyId'),
                        aws_secret_access_key=credentials.get('SecretAccessKey'),
                        aws_session_token=credentials.get('SessionToken'),
                        region_name=AWS_REGION
                    )
                    
                    return True
                    
        except Exception as cli_err:
            print(f"Erro ao obter credenciais via AWS CLI: {cli_err}")
            # Continue para o próximo método
        
        # Se a abordagem anterior falhar, tente obter via boto3
        print("Tentando obter credenciais via boto3...")
        
        # Configurar boto3 com o perfil
        boto3.setup_default_session(profile_name=profile_name)
        
        # Obter credenciais da sessão
        session = boto3.Session(profile_name=profile_name)
        credentials = session.get_credentials()
        frozen_credentials = credentials.get_frozen_credentials()
        
        if frozen_credentials:
            # Definir variáveis de ambiente com credenciais congeladas
            os.environ['AWS_ACCESS_KEY_ID'] = frozen_credentials.access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = frozen_credentials.secret_key
            if hasattr(frozen_credentials, 'token') and frozen_credentials.token:
                os.environ['AWS_SESSION_TOKEN'] = frozen_credentials.token
            
            # Configure also Hadoop system properties for S3A
            os.environ['HADOOP_ACCESS_KEY'] = frozen_credentials.access_key
            os.environ['HADOOP_SECRET_KEY'] = frozen_credentials.secret_key
            if hasattr(frozen_credentials, 'token') and frozen_credentials.token:
                os.environ['HADOOP_SESSION_TOKEN'] = frozen_credentials.token
            
            # Configure AWS system properties (alternative approach)
            os.environ['aws.accessKeyId'] = frozen_credentials.access_key
            os.environ['aws.secretKey'] = frozen_credentials.secret_key
            
            # Imprimir informações para debug
            print(f"Variáveis de ambiente configuradas via boto3.")
            print(f"  AWS_ACCESS_KEY_ID: {frozen_credentials.access_key[:4]}****")
            print(f"  AWS_SECRET_ACCESS_KEY: {frozen_credentials.secret_key[:4]}****")
            if hasattr(frozen_credentials, 'token') and frozen_credentials.token:
                print(f"  AWS_SESSION_TOKEN: Configurado")
                
            # Verificação adicional
            if not os.environ.get('AWS_ACCESS_KEY_ID'):
                print("AVISO: AWS_ACCESS_KEY_ID não está definido no ambiente mesmo após configuração!")
                
            return True
    except Exception as e:
        print(f"Erro ao obter credenciais SSO: {e}")
    
    return False

def get_aws_secret(secret_name=SECRET_NAME, region_name=AWS_REGION):
    """
    Obtém um segredo do AWS Secrets Manager.
    
    Args:
        secret_name (str): Nome do segredo no Secrets Manager.
        region_name (str): Região AWS onde o segredo está armazenado.
        
    Returns:
        dict: Os dados do segredo em formato dicionário.
        
    Raises:
        Exception: Se ocorrer um erro ao acessar o segredo.
    """
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except Exception as e:
        print(f"Erro ao obter segredo {secret_name}: {e}")
        raise

def update_hadoop_configuration(spark_context):
    """
    Atualiza a configuração do Hadoop para usar credenciais AWS corretas.
    
    Args:
        spark_context: SparkContext a ser configurado.
        
    Returns:
        bool: True se a configuração foi atualizada com sucesso.
    """
    try:
        # Obtém acesso à configuração do Hadoop
        hadoop_conf = spark_context._jsc.hadoopConfiguration()
        
        # Configura credenciais AWS no Hadoop
        if os.environ.get('AWS_ACCESS_KEY_ID'):
            hadoop_conf.set("fs.s3a.access.key", os.environ.get('AWS_ACCESS_KEY_ID'))
            hadoop_conf.set("fs.s3.access.key", os.environ.get('AWS_ACCESS_KEY_ID'))
            
        if os.environ.get('AWS_SECRET_ACCESS_KEY'):
            hadoop_conf.set("fs.s3a.secret.key", os.environ.get('AWS_SECRET_ACCESS_KEY'))
            hadoop_conf.set("fs.s3.secret.key", os.environ.get('AWS_SECRET_ACCESS_KEY'))
            
        if os.environ.get('AWS_SESSION_TOKEN'):
            hadoop_conf.set("fs.s3a.session.token", os.environ.get('AWS_SESSION_TOKEN'))
            hadoop_conf.set("fs.s3.session.token", os.environ.get('AWS_SESSION_TOKEN'))
        
        # Configura outros parâmetros importantes
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # Força o uso do SimpleAWSCredentialsProvider
        hadoop_conf.set("fs.s3a.aws.credentials.provider", 
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        # Configura endpoint padrão
        hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
        
        # Log importante sobre as configurações
        access_key = hadoop_conf.get("fs.s3a.access.key")
        access_key_masked = access_key[:4] + "****" if access_key else "Não definido"
        
        print("\nConfiguração do Hadoop atualizada:")
        print(f"  fs.s3a.access.key = {access_key_masked}")
        print(f"  fs.s3a.aws.credentials.provider = {hadoop_conf.get('fs.s3a.aws.credentials.provider')}")
        print(f"  fs.s3a.impl = {hadoop_conf.get('fs.s3a.impl')}")
        print(f"  fs.s3.impl = {hadoop_conf.get('fs.s3.impl')}")
        
        return True
    except Exception as e:
        print(f"Erro ao atualizar configuração do Hadoop: {e}")
        return False