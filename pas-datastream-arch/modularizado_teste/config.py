"""
Arquivo de configurações globais para a aplicação.
"""

# Configurações S3
BUCKET_NAME = 'pas-prod-landing-zone'

# Prefixos S3
KEY_PREFIX_RAW = 'streaming/validacao-wicco/'
KEY_PREFIX_EVENT_LOG = 'streaming/lrs_baa/event/'
KEY_PREFIX_UNKNOWN = 'streaming/lrs_baa/verbid_unknown/'
KEY_PREFIX_PROCESSED = 'streaming/lrs_baa/verb_id/'
KEY_PREFIX_ERROR = 'streaming/lrs_baa/event_error/redshift_conn/'

# Configurações Kinesis
KINESIS_NAME = "pas-datastream-lrs-baa"

# Configuração do Redshift
REDSHIFT_CONFIG = {
    'dbname': 'datalake',
    'user': 'admin',
    'password': 'IVDMBvybjo842++',  # Idealmente, usar AWS Secrets Manager
    'host': 'pas-prod-redshift-workgroup.888577054267.us-east-1.redshift-serverless.amazonaws.com',
    'port': 5439,
    'sslmode': 'require',
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5
}

# Configurações de processamento
BATCH_SIZE = 1  # Define o tamanho do lote
MAX_RETRIES = 3  # Número máximo de tentativas para reconexão