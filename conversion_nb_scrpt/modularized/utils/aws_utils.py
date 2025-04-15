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
            return True
            
        # Executar comando para obter credenciais
        print(f"Tentando obter credenciais para o perfil {profile_name}...")
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity", "--profile", profile_name, "--output", "json"],
            capture_output=True, text=True, check=True
        )
        identity = json.loads(result.stdout)
        print(f"Conectado como: {identity.get('Arn', 'Desconhecido')}")
        
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
            
            # Imprimir informações para debug
            print(f"Variáveis de ambiente configuradas.")
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