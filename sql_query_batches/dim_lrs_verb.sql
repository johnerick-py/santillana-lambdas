-- gold.dim_lrs_verb
SELECT
   event.id::VARCHAR AS statement_id,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.statement.verb.id::VARCHAR AS verb_id,
   event.statement.verb.display.und::VARCHAR AS display_und,
   event.statement.verb.display.enus::VARCHAR AS display_en_us,
   event.statement.verb.display.eses::VARCHAR AS display_es_es,
   event.statement.verb.display.dede::VARCHAR AS display_de_de,
   event.statement.verb.display.es::VARCHAR AS display_es,
   event.statement.verb.display.engb::VARCHAR AS display_en_gb,
   event.statement.verb.display.frfr::VARCHAR AS display_fr_fr
FROM
   stream_raw.lrs_events_all
   
   
REDSHIFT_HOST = "pas-prod-redshift-workgroup.888577054267.us-east-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = "5439"
REDSHIFT_DB = "datalake"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "IVDMBvybjo842++"