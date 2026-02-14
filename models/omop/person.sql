MODEL (
  name lth_bronze.person,
  cron '@daily',
  kind FULL
);

SELECT
  p.person_id::BIGINT AS person_id,
  p.gender_concept_id::BIGINT AS gender_concept_id,
  datepart(year, p.birth_datetime)::INTEGER AS year_of_birth,
  datepart(month, p.birth_datetime)::INTEGER AS month_of_birth,
  datepart(day, p.birth_datetime)::INTEGER AS day_of_birth,
  p.birth_datetime::DATETIME AS birth_datetime,
  p.race_concept_id::BIGINT AS race_concept_id,
  p.ethnicity_concept_id::BIGINT AS ethnicity_concept_id,
  vl.location_id::BIGINT AS location_id,
  pr.provider_id::BIGINT AS provider_id,
  cs.care_site_id::BIGINT AS care_site_id,
  p.person_source_value::VARCHAR(50) AS person_source_value,
  p.gender_source_value::VARCHAR(50) AS gender_source_value,
  NULL::BIGINT AS gender_source_concept_id,
  p.race_source_value::VARCHAR(50) AS race_source_value,
  NULL::BIGINT AS race_source_concept_id,
  NULL::VARCHAR(50) AS ethnicity_source_value,
  NULL::BIGINT AS ethnicity_source_concept_id,
  p.source_system::VARCHAR(20),
  p.org_code::VARCHAR(5),
  last_edit_time::DATETIME,
  getdate()::DATETIME AS insert_date_time
FROM lth_bronze.vocab__person AS p
LEFT JOIN (
  SELECT DISTINCT
    provider_id AS provider_id,
    provider_source_value AS provider_source_value
  FROM lth_bronze.provider
) AS pr
  ON p.provider_id = pr.provider_source_value
LEFT JOIN (
  SELECT DISTINCT
    care_site_id AS care_site_id,
    care_site_source_value AS care_site_source_value
  FROM lth_bronze.care_site
) AS cs
  ON p.gp_prac_code = cs.care_site_source_value
LEFT JOIN lth_bronze.location AS vl
  ON p.mailing_code = vl.location_source_value
