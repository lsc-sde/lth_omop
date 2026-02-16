MODEL (
  name cdm.specimen,
  kind FULL,
  cron '@daily'
);

SELECT
  row_number() OVER (ORDER BY NEWID())::BIGINT AS specimen_id,
  person_id::BIGINT AS person_id,
  specimen_concept_id::BIGINT AS specimen_concept_id,
  specimen_type_concept_id::BIGINT AS specimen_type_concept_id,
  specimen_date::DATE AS specimen_date,
  specimen_date::DATETIME AS specimen_datetime,
  NULL::FLOAT AS quantity,
  NULL::BIGINT AS unit_concept_id,
  anatomic_site_concept_id::BIGINT AS anatomic_site_concept_id,
  NULL::BIGINT AS disease_status_concept_id,
  specimen_source_id::VARCHAR(50) AS specimen_source_id,
  specimen_source_value::VARCHAR(50) AS specimen_source_value,
  NULL::VARCHAR(50) AS unit_source_value,
  anatomic_site_source_value::VARCHAR(50) AS anatomic_site_source_value,
  NULL::VARCHAR(50) AS disease_status_source_value,
  measurement_event_id AS specimen_event_id,
  updated_at::DATETIME AS updated_at
FROM vcb.vocab__specimen
