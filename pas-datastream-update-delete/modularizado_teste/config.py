"""
Arquivo de configurações globais para a aplicação.
"""

# Configurações S3
BUCKET_NAME = 'pas-prod-landing-zone'

# Prefixos S3
KEY_PREFIX_RAW = 'streaming_pas/validacao-wicco/'
KEY_PREFIX_EVENT_LOG = 'streaming_pas/event_updated_deleted/'
KEY_PREFIX_UNKNOWN = 'streaming_pas/entitykey_unknown_updated_deleted/'
KEY_PREFIX_PROCESSED = 'streaming_pas/entity_key_updated_deleted/'
KEY_PREFIX_ERROR = 'streaming_pas/event_error_updated_deleted/redshift_conn/'

# Configurações Kinesis
KINESIS_NAME = "pas-datastream"

# Configuração do Redshift
REDSHIFT_CONFIG = {
    'dbname': 'datalake',
    'user': '',
    'password': '',  # Idealmente, usar AWS Secrets Manager
    'host': '',
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