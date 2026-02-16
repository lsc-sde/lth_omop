MODEL (
  name cdm.measurement,
  cron '@daily',
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column last_edit_time,
    batch_size 30,
    batch_concurrency 4
  )
);

SELECT
  @generate_surrogate_key(
    vm.source_system,
    vm.person_id,
    vm.measurement_event_id,
    vm.target_concept_id,
    source_value,
    value_source_value
  )::VARBINARY(16) AS measurement_id,
  vm.person_id AS person_id,
  vm.target_concept_id::BIGINT AS measurement_concept_id,
  vm.result_datetime AS measurement_date,
  vm.result_datetime AS measurement_datetime,
  vm.result_datetime AS measurement_time,
  vm.type_concept_id::BIGINT AS measurement_type_concept_id,
  vm.operator_concept_id::BIGINT AS operator_concept_id,
  vm.value_as_number AS value_as_number,
  vm.value_as_concept_id::BIGINT AS value_as_concept_id,
  vm.unit_concept_id::BIGINT AS unit_concept_id,
  vm.range_low AS range_low,
  vm.range_high AS range_high,
  pr.provider_id::BIGINT AS provider_id,
  vm.visit_occurrence_id::BIGINT AS visit_occurrence_id,
  vm.visit_detail_id::BIGINT AS visit_detail_id,
  vm.source_value AS measurement_source_value,
  vm.measurement_source_concept_id::BIGINT AS measurement_source_concept_id,
  vm.unit_source_value AS unit_source_value,
  vm.unit_source_concept_id::BIGINT AS unit_source_concept_id,
  vm.value_source_value AS value_source_value,
  vm.meas_event_field_concept_id::BIGINT AS meas_event_field_concept_id,
  vm.measurement_event_id,
  vm.org_code,
  vm.source_system,
  vm.updated_at,
  vm.last_edit_time,
  getdate() AS insert_date_time
FROM vcb.vocab__measurement AS vm
LEFT JOIN cdm.provider AS pr
  ON vm.provider_id = pr.provider_source_value
WHERE
  vm.last_edit_time BETWEEN @start_ds AND @end_ds
