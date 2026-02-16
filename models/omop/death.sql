MODEL (
  name cdm.death,
  kind FULL,
  cron '@daily'
);

SELECT
  person_id::BIGINT AS person_id,
  death_date::DATE AS death_date,
  death_datetime::DATETIME AS death_datetime,
  death_type_concept_id::BIGINT AS death_type_concept_id,
  NULL::BIGINT AS cause_concept_id,
  NULL::VARCHAR(50) AS cause_source_value,
  NULL::BIGINT AS cause_source_concept_id,
  source_system::VARCHAR(20),
  org_code::VARCHAR(5)
FROM vcb.vocab__death
