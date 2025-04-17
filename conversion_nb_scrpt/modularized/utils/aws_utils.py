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
    Opcional: Obtém credenciais AWS através do SSO e configura variáveis de ambiente.
    Não deve ser usado se estiver utilizando Access Key e Secret Key diretamente.
    """
    if not profile_name:
        print("🔸 Nenhum perfil SSO informado. Pulando configuração de perfil.")
        return False

    try:
        if profile_name.startswith("profile "):
            profile_name = profile_name.replace("profile ", "")
            print(f"Removido prefixo 'profile ': {profile_name}")
        
        print(f"Verificando identidade AWS para o perfil '{profile_name}'...")
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity", "--profile", profile_name, "--output", "json"],
            capture_output=True, text=True
        )

        if result.returncode != 0:
            print(f"⚠️ Falha ao verificar identidade com o perfil: {result.stderr}")
            return False

        identity = json.loads(result.stdout)
        print(f"✅ Conectado como: {identity.get('Arn')}")

        session = boto3.Session(profile_name=profile_name)
        credentials = session.get_credentials()
        if not credentials:
            print("❌ Nenhuma credencial encontrada.")
            return False

        frozen = credentials.get_frozen_credentials()
        os.environ['AWS_ACCESS_KEY_ID'] = frozen.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = frozen.secret_key
        if frozen.token:
            os.environ['AWS_SESSION_TOKEN'] = frozen.token

        print("✅ Variáveis de ambiente AWS configuradas com sucesso.")
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
        # Corrigir nomes de variáveis (caso .env use nomes incorretos)
        os.environ['AWS_ACCESS_KEY_ID'] = os.environ.get('AWS_ACCESS_KEY_ID') or os.environ.get('AWS_ACCESS_KEY', '')
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.environ.get('AWS_SECRET_ACCESS_KEY') or os.environ.get('AWS_SECRET_KEY', '')

        # Obtém acesso à configuração do Hadoop
        hadoop_conf = spark_context._jsc.hadoopConfiguration()
        
        # Força o uso da cadeia de credenciais padrão da AWS
        hadoop_conf.set("fs.s3a.aws.credentials.provider", 
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        
        # Configura credenciais AWS no Hadoop
        if os.environ.get('AWS_ACCESS_KEY_ID'):
            hadoop_conf.set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
            hadoop_conf.set("fs.s3.access.key", os.environ['AWS_ACCESS_KEY_ID'])
            
        if os.environ.get('AWS_SECRET_ACCESS_KEY'):
            hadoop_conf.set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
            hadoop_conf.set("fs.s3.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
            
        if os.environ.get('AWS_SESSION_TOKEN'):
            hadoop_conf.set("fs.s3a.session.token", os.environ['AWS_SESSION_TOKEN'])
            hadoop_conf.set("fs.s3.session.token", os.environ['AWS_SESSION_TOKEN'])
        
        # Configurações obrigatórias do S3
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        
        # Configurações de tempo limite
        hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
        hadoop_conf.set("fs.s3a.connection.timeout", "10000")
        hadoop_conf.set("fs.s3a.attempts.maximum", "20")
        
        # Log de debug
        access_key = hadoop_conf.get("fs.s3a.access.key")
        provider = hadoop_conf.get("fs.s3a.aws.credentials.provider")
        access_key_masked = access_key[:4] + "****" if access_key else "Não definido"
        
        print("\n🔧 Configuração do Hadoop atualizada:")
        print(f"  fs.s3a.access.key = {access_key_masked}")
        print(f"  fs.s3a.aws.credentials.provider = {provider}")
        print(f"  fs.s3a.impl = {hadoop_conf.get('fs.s3a.impl')}")
        
        return True
    except Exception as e:
        print(f"Erro ao atualizar configuração do Hadoop: {e}")
        return False
