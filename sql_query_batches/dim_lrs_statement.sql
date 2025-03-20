-- gold.dim_lrs_statement
SELECT
   event.id::VARCHAR AS statement_id,
   event.active::VARCHAR AS active_status,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.relatedactivities[0]::VARCHAR AS related_activities,
   event.statement.stored::VARCHAR AS date_stored
FROM
   stream_raw.lrs_events_all