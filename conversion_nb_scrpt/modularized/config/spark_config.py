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
    
    print(f"Variáveis AWS configuradas: {'Sim' if aws_access_key else 'Não'}")
    
    # Criar sessão Spark com configurações otimizadas
    spark = (
        SparkSession.builder
        .appName("WiccoToGoldParquet")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.cores", "16")
        .config("spark.default.parallelism", "200")
        .config("spark.dynamicAllocation.enabled", "true") 
        .config("spark.dynamicAllocation.minExecutors", "4")
        .config("spark.dynamicAllocation.maxExecutors", "20")
        .config("spark.sql.warehouse.dir", f"{LOCAL_TEMP_DIR}/spark-warehouse")
        .config("spark.local.dir", f"{LOCAL_TEMP_DIR}/spark-local")
        # Configurações críticas para S3
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.session.token", aws_session_token)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Importante: mapeamento de s3:// para S3AFileSystem
        .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Mapeamento extra
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "10000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "15000")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    # Configurações adicionais
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    # Teste se a conexão com S3 está funcionando
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    
    print("Configuração do sistema de arquivos Hadoop:")
    print(f"  fs.s3.impl = {hadoop_conf.get('fs.s3.impl')}")
    print(f"  fs.s3a.impl = {hadoop_conf.get('fs.s3a.impl')}")
    print(f"  fs.s3a.access.key = {'*' * 8 if aws_access_key else 'Não definido'}")
    
    print("Sessão Spark inicializada com sucesso.")
    return spark