-- tabela dim_lrs_object_http
SELECT
   event.id::VARCHAR AS statement_id,
   event.lrsid::VARCHAR AS lrs_id,
   event.clientid::VARCHAR AS client_id,
   event.statement.object.definition.extensions."http://acttype".activityrefid::VARCHAR AS activity_id,
   event.statement.object.definition.extensions."http://acttype".resourceid::VARCHAR AS resource_ref_id,
   event.statement.object.definition.extensions."http://acttype".resourcerefid::VARCHAR AS resource_id,
   event.statement.object.definition.extensions."http://acttype".sourcerefid::VARCHAR AS source_id,
   event.statement.object.definition.extensions."http://acttype".learningunitrefid::VARCHAR AS learning_unit_id,
   event.statement.object.definition.extensions."http://acttype".sectionsubjectrefid::VARCHAR AS section_subject_id,
   event.statement.object.definition.extensions."http://acttype".schoolclassid::VARCHAR AS school_class_id,
   event.statement.object.definition.extensions."http://acttype".learningobjectiverefid::VARCHAR AS learning_objective_id,
   event.statement.object.definition.extensions."http://acttype".typeactivityintelligence::VARCHAR AS type_intelligence,
   event.statement.object.definition.extensions."http://acttype".pagerefid::VARCHAR AS page_ref_id,
   event.statement.object.definition.extensions."http://acttype".studentrefid::VARCHAR AS student_id,
   event.statement.object.definition.extensions."http://acttype".rolid::VARCHAR AS role_id,
   event.statement.object.definition.extensions."http://acttype".levelcode::VARCHAR AS level_code,
   event.statement.object.definition.extensions."http://acttype".learninglevelrefid::VARCHAR AS learning_level_id,
   event.statement.object.definition.extensions."http://acttype".adventurecode::VARCHAR AS adventure_code,
   event.statement.object.definition.extensions."http://acttype".typeactivityvalue::VARCHAR AS activity_type,
   event.statement.object.definition.extensions."http://acttype".acttype::VARCHAR AS act_type,
   event.statement.object.definition.extensions."http://acttype".unitid::VARCHAR AS unit_id,
   event.statement.object.definition.extensions."http://acttype".learningobjectiverefidapi::VARCHAR AS learning_objective_id_api,
   event.statement.object.definition.extensions."http://acttype".subjectgraderefid::VARCHAR AS subject_grade_id,
   event.statement.object.definition.extensions."http://acttype".schoolrefid::VARCHAR AS school_id
FROM
   stream_raw.lrs_events_all
WHERE event.statement.object.definition.extensions."http://acttype" is not null