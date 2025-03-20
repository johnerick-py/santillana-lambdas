-- gold.dim_lrs_result
SELECT
   event.id::VARCHAR AS statement_id,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.statement.result.score."raw"::VARCHAR AS score_raw,
   event.statement.result.score."max"::VARCHAR AS score_max,
   event.statement.result.score."min"::VARCHAR AS score_min,
   event.statement.result.response::VARCHAR AS response,
   event.statement.result.success::BOOL AS success_status
FROM
   stream_raw.lrs_events_all