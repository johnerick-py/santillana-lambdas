SELECT
   event.id::VARCHAR AS statement_id,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.statement.authority.name::VARCHAR AS authority_name,
   event.statement.authority.mbox::VARCHAR AS authority_mbox
FROM
   stream_raw.lrs_events_answered