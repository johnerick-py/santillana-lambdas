-- gold.dim_lrs_object
SELECT
   event.id::VARCHAR AS statement_id,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.statement.object.id::VARCHAR AS object_id,
   event.statement.object.objectType::VARCHAR AS object_type,
   event.statement.object.definition.interactiontype::VARCHAR AS interaction_type,
   event.statement.object.definition.description.es::VARCHAR AS definition_description_es
FROM
   stream_raw.lrs_events_all