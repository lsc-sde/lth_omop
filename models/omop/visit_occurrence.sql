MODEL (
  name lth_bronze.visit_occurrence,
  cron '@daily',
  kind FULL
);

SELECT
  visit_occurrence_id::BIGINT AS visit_occurrence_id,
  person_id::BIGINT AS person_id,
  visit_concept_id::BIGINT AS visit_concept_id,
  visit_start_datetime::DATE AS visit_start_date,
  visit_start_datetime::DATETIME AS visit_start_datetime,
  visit_end_datetime::DATE AS visit_end_date,
  visit_end_datetime::DATETIME AS visit_end_datetime,
  visit_type_concept_id::BIGINT AS visit_type_concept_id,
  pr.provider_id::BIGINT AS provider_id,
  cs.care_site_id::BIGINT AS care_site_id,
  visit_source_value::VARCHAR(50) AS visit_source_value,
  visit_source_concept_id::BIGINT AS visit_source_concept_id,
  admitted_from_concept_id::BIGINT AS admitted_from_concept_id,
  admitted_from_source_value::VARCHAR(50) AS admitted_from_source_value,
  discharged_to_concept_id::BIGINT AS discharged_to_concept_id,
  discharged_to_source_value::VARCHAR(50) AS discharged_to_source_value,
  lag(visit_occurrence_id) OVER (PARTITION BY person_id ORDER BY visit_start_datetime) AS preceding_visit_occurrence_id,
  vo.visit_type_id,
  vo.visit_status_id,
  vo.source_system::VARCHAR(20),
  vo.org_code::VARCHAR(5),
  vo.last_edit_time,
  vo.updated_at
FROM lth_bronze.vocab__visit_occurrence AS vo
LEFT JOIN lth_bronze.provider AS pr
  ON vo.provider_id::VARCHAR = pr.provider_source_value
LEFT JOIN (
  SELECT
    *
  FROM lth_bronze.care_site
  WHERE
    place_of_service_source_value = 'NHS Trust'
) AS cs
  ON vo.care_site_id::VARCHAR = cs.care_site_source_value
