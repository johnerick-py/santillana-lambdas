-- gold.dim_lrs_scorm
SELECT
   event.id::VARCHAR AS statement_id,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.statement.result.extensions."http://scorm&46;com/extensions/usadata".multiplerecording::VARCHAR AS recording,
   event.statement.result.extensions."http://scorm&46;com/extensions/usadata".correctresponsespattern::VARCHAR AS pattern_response,
   event.statement.result.extensions."http://scorm&46;com/extensions/usadata".response::VARCHAR AS response
FROM
   stream_raw.lrs_events_answered