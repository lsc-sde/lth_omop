MODEL (
  name cdm.observation,
  kind FULL,
  cron '@daily',
  kind FULL
);

SELECT
  @generate_surrogate_key(
    vo.source_system,
    person_id,
    vo.provider_id,
    observation_event_id,
    visit_occurrence_id,
    source_value,
    value_source_value,
    result_datetime,
    target_concept_id,
    qualifier_concept_id
  )::VARBINARY(16) AS observation_id,
  person_id,
  target_concept_id::BIGINT AS observation_concept_id,
  result_datetime::DATE AS observation_date,
  result_datetime::DATETIME AS observation_datetime,
  type_concept_id::BIGINT AS observation_type_concept_id,
  TRY_CAST(value_as_number AS FLOAT) AS value_as_number,
  value_as_string::VARCHAR(60) AS value_as_string,
  value_as_concept_id::BIGINT AS value_as_concept_id,
  qualifier_concept_id::BIGINT AS qualifier_concept_id,
  unit_concept_id::BIGINT AS unit_concept_id,
  pr.provider_id::BIGINT AS provider_id,
  visit_occurrence_id::BIGINT AS visit_occurrence_id,
  visit_detail_id::BIGINT AS visit_detail_id,
  source_value::VARCHAR(50) AS observation_source_value,
  observation_source_concept_id::BIGINT AS observation_source_concept_id,
  unit_source_value::VARCHAR(50) AS unit_source_value,
  qualifier_source_value::VARCHAR(50) AS qualifier_source_value,
  value_source_value::VARCHAR(50) AS value_source_value,
  observation_event_id AS observation_event_id,
  obs_event_field_concept_id::BIGINT AS obs_event_field_concept_id,
  vo.org_code,
  vo.source_system,
  last_edit_time,
  getdate() AS insert_date_time
FROM vcb.vocab__observation AS vo
LEFT JOIN cdm.provider AS pr
  ON vo.provider_id = pr.provider_source_value
