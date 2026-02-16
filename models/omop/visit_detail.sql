MODEL (
  name cdm.visit_detail,
  kind FULL,
  cron '@daily'
);

SELECT
  visit_detail_id::BIGINT AS visit_detail_id,
  person_id::BIGINT AS person_id,
  visit_detail_concept_id::BIGINT AS visit_detail_concept_id,
  visit_detail_start_datetime::DATE AS visit_detail_start_date,
  visit_detail_start_datetime::DATETIME AS visit_detail_start_datetime,
  visit_detail_end_datetime::DATE AS visit_detail_end_date,
  visit_detail_end_datetime::DATETIME AS visit_detail_end_datetime,
  visit_detail_type_concept_id::BIGINT AS visit_detail_type_concept_id,
  provider_id::BIGINT AS provider_id,
  cs.care_site_id::BIGINT AS care_site_id,
  visit_detail_source_value::VARCHAR(50) AS visit_detail_source_value,
  visit_detail_source_concept_id::BIGINT AS visit_detail_source_concept_id,
  admitted_from_concept_id::BIGINT AS admitted_from_concept_id,
  admitted_from_source_value::VARCHAR(50) AS admitted_from_source_value,
  discharged_to_source_value::VARCHAR(50) AS discharged_to_source_value,
  discharged_to_concept_id::BIGINT AS discharged_to_concept_id,
  preceding_visit_detail_id::BIGINT AS preceding_visit_detail_id,
  parent_visit_detail_id::BIGINT AS parent_visit_detail_id,
  visit_occurrence_id::BIGINT AS visit_occurrence_id,
  vd.source_system::VARCHAR(20),
  vd.org_code::VARCHAR(5)
FROM vcb.vocab__visit_detail AS vd
LEFT JOIN cdm.care_site AS cs
  ON vd.location_id = cs.care_site_source_value
