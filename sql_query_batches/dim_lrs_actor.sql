SELECT
   event.id::VARCHAR AS statement_id,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.statement.actor.name::VARCHAR AS actor_name,
   event.statement.actor.mbox::VARCHAR AS actor_mbox,
   event.statement.actor.objecttype::VARCHAR AS actor_type
FROM
   stream_raw.lrs_events_all