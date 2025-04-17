import os
from pyspark.sql import SparkSession
from config.settings import LOCAL_TEMP_DIR

# Define região AWS padrão
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

def initialize_spark():
    """
    Inicializa a sessão Spark com configurações otimizadas de memória.
    """
    print("Inicializando sessão Spark com credenciais fixas...")

    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')

    spark = (
        SparkSession.builder
        .appName("WiccoToGoldParquet")

        # Recursos mínimos para evitar uso excessivo de RAM
        .config("spark.executor.cores", "6")
        .config("spark.executor.memory", "6g")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.adaptive.enabled", "true")

        # Paralelismo reduzido para controlar uso de CPU
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.default.parallelism", "24")

        # Evita alocação dinâmica de executores que pode consumir memória excessiva
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.local.dir", "/home/drchapeu/Documents/ajuda_school_level/modularized/tmp/spark-temp")

        # AWS e S3 configs
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        # Memória offHeap para evitar estourar heap do driver
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")

        # Compressão
        .config("spark.sql.parquet.compression.codec", "snappy")

        .enableHiveSupport()
        .getOrCreate()
    )

    # Diretório de checkpoint para controle de memória e falhas
    spark.sparkContext.setCheckpointDir("/home/drchapeu/Documents/ajuda_school_level/modularized/tmp/spark-checkpoint")

    # Configurações extras
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.sparkContext.setLogLevel("WARN")

    # Configurações AWS adicionais no HadoopConf
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)

    print("✅ Sessão Spark inicializada com credenciais fixas.")
    return spark



# def initialize_spark():
#     """
#     Inicializa a sessão Spark com as configurações necessárias.
    
#     Returns:
#         SparkSession: A sessão Spark inicializada.
#     """
#     print("Inicializando sessão Spark...")
    
#     # Garantir que o diretório temporário local existe
#     os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    
#     # Verificar se temos as variáveis de ambiente AWS definidas
#     aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
#     aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
#     aws_session_token = os.environ.get('AWS_SESSION_TOKEN', '')
    
#     print(f"Variáveis AWS disponíveis antes da inicialização: {'Sim' if aws_access_key else 'Não'}")
#     print(f"AWS_ACCESS_KEY_ID: {aws_access_key[:4] + '****' if aws_access_key else 'NÃO DEFINIDO'}")
#     print(f"AWS_SECRET_ACCESS_KEY: {'DEFINIDO' if aws_secret_key else 'NÃO DEFINIDO'}")
#     print(f"AWS_SESSION_TOKEN: {'DEFINIDO' if aws_session_token else 'NÃO DEFINIDO'}")
    
#     # Criar sessão Spark com configurações otimizadas - usando configuração similar
#     # ao código que está funcionando
#     spark = (
#         SparkSession.builder
#         .appName("WiccoToGoldParquet")
#         # Configurações de recursos
#         .config("spark.sql.shuffle.partitions", "100")
#         .config("spark.executor.memory", "4g")
#         .config("spark.driver.memory", "4g")
#         .config("spark.sql.adaptive.enabled", "true")
#         .config("spark.driver.cores", "16")
#         .config("spark.default.parallelism", "50")
#         .config("spark.dynamicAllocation.enabled", "true")
#         .config("spark.dynamicAllocation.minExecutors", "1")
#         .config("spark.dynamicAllocation.maxExecutors", "6")
#         .config("spark.memory.offHeap.enabled", "true")
#         .config("spark.memory.offHeap.size", "1g")
#         # Configurações para pacotes
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
#         # Configurações AWS S3 - MUDANÇA IMPORTANTE: usando DefaultAWSCredentialsProviderChain
#         .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
#         .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config("spark.hadoop.fs.s3a.connection.establish.timeout", "10000")
#         .config("spark.hadoop.fs.s3a.connection.timeout", "20000")
#         # Outras configurações
#         .config("spark.sql.parquet.compression.codec", "snappy")
#         .enableHiveSupport()
#         .getOrCreate()
#     )
    
#     # Configurações adicionais
#     spark.conf.set("spark.sql.parquet.binaryAsString", "true")
#     spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
#     # Definir o nível de log
#     spark.sparkContext.setLogLevel("WARN")
    
#     # Configuração adicional direta no Hadoop
#     hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
#     # Configurar explicitamente as credenciais se estiverem disponíveis
#     if aws_access_key:
#         hadoop_conf.set("fs.s3a.access.key", aws_access_key)
#         hadoop_conf.set("fs.s3.access.key", aws_access_key)
#     if aws_secret_key:
#         hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
#         hadoop_conf.set("fs.s3.secret.key", aws_secret_key)
#     if aws_session_token:
#         hadoop_conf.set("fs.s3a.session.token", aws_session_token)
#         hadoop_conf.set("fs.s3.session.token", aws_session_token)
    
#     # Reforçar o provider de credenciais
#     hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    
#     # Mostrar as configurações atuais para debug
#     access_key = hadoop_conf.get("fs.s3a.access.key")
#     provider = hadoop_conf.get("fs.s3a.aws.credentials.provider")
    
#     if access_key:
#         print(f"Hadoop configurado com chave: {access_key[:4]}****")
#     else:
#         print(f"Hadoop usando provider: {provider}")
    
#     print("Sessão Spark inicializada com sucesso.")
#     return spark

