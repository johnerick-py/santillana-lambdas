#!/usr/bin/env python
# coding: utf-8

"""
Configurações do Spark para o projeto.
"""

import os
from pyspark.sql import SparkSession
from config.settings import LOCAL_TEMP_DIR

def initialize_spark():
    """
    Inicializa a sessão Spark com as configurações necessárias.
    
    Returns:
        SparkSession: A sessão Spark inicializada.
    """
    print("Inicializando sessão Spark...")
    
    # Garantir que o diretório temporário local existe
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    
    # Verificar se temos as variáveis de ambiente AWS definidas
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    aws_session_token = os.environ.get('AWS_SESSION_TOKEN', '')
    
    print(f"Variáveis AWS disponíveis antes da inicialização: {'Sim' if aws_access_key else 'Não'}")
    print(f"AWS_ACCESS_KEY_ID: {aws_access_key[:4] + '****' if aws_access_key else 'NÃO DEFINIDO'}")
    print(f"AWS_SECRET_ACCESS_KEY: {'DEFINIDO' if aws_secret_key else 'NÃO DEFINIDO'}")
    print(f"AWS_SESSION_TOKEN: {'DEFINIDO' if aws_session_token else 'NÃO DEFINIDO'}")
    
    # Criar sessão Spark com configurações otimizadas
    spark = (
        SparkSession.builder
        .appName("WiccoToGoldParquet")
        # Configurações de recursos
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.cores", "16")
        .config("spark.default.parallelism", "200")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "4")
        .config("spark.dynamicAllocation.maxExecutors", "20")
        # Configurações para pacotes
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
        # Configurações AWS S3
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.session.token", aws_session_token)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        # Outras configurações
        .config("spark.sql.parquet.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    # Configurações adicionais
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    # Configuração adicional direta no Hadoop
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.session.token", aws_session_token)
    hadoop_conf.set("fs.s3.access.key", aws_access_key)
    hadoop_conf.set("fs.s3.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3.session.token", aws_session_token)
    
    # Para depuração - mostrar as credenciais configuradas
    access_key = hadoop_conf.get("fs.s3a.access.key")
    if access_key:
        print(f"Hadoop configurado com chave: {access_key[:4]}****")
    else:
        print("AVISO: Hadoop configurado com chave de acesso vazia!")
    
    print("Sessão Spark inicializada com sucesso.")
    return spark