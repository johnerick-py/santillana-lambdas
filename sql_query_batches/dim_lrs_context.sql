-- tabela gold.dim_lrs_context
SELECT
   event.id::VARCHAR AS statement_id,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.statement.context.contextactivities.parent[0].id::VARCHAR AS context_parent,
   event.statement.context.contextactivities.parent[0].objecttype::VARCHAR AS context_parent_object_type
FROM
   stream_raw.lrs_events_answered