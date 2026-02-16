MODEL (
  name stg.stg_flex__result,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column last_edit_time,
    batch_size 30,
    batch_concurrency 4
  ),
  cron '@daily'
);

WITH visit_detail AS (
  SELECT DISTINCT
    visit_number AS visit_number,
    visit_id AS visit_id,
    first_visit_id AS first_visit_id,
    person_source_value AS person_source_value
  FROM stg.stg_flex__facility_transfer
)
SELECT DISTINCT
  fr.person_source_value AS flex_patient_id,
  fr.event_date_time AS result_datetime,
  fr.emp_provider_id AS provider_id,
  fr.data_element_id AS source_code,
  fr.field_name AS source_name,
  fr.procedure_name,
  fr.result_value AS value_source_value,
  fr.trimmed_result_value AS source_value,
  CASE
    WHEN result_value LIKE 'Score %' AND len(result_value) = 7
    THEN replace(result_value, 'Score ', '')
    ELSE replace(replace(fr.trimmed_result_value, '<', ''), '>', '')
  END::FLOAT AS value_as_number,
  fr.display_unit AS unit_source_value,
  source_system::VARCHAR(20),
  org_code::VARCHAR(5),
  fr.last_edit_time,
  fr.updated_at,
  isnull(vd.first_visit_id, fr.visit_id)::NUMERIC(12, 0) AS visit_occurrence_id,
  concat(fr.visit_id, fr.event_id)::VARCHAR(82) AS measurement_event_id
FROM src.src_flex__result AS fr
LEFT JOIN visit_detail AS vd
  ON fr.visit_id = vd.visit_id
WHERE
  fr.last_edit_time BETWEEN @start_ds AND @end_ds
