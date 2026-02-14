MODEL (
  name lth_bronze.provider,
  kind FULL,
  cron '@daily'
);

SELECT DISTINCT
  provider_id::BIGINT AS provider_id,
  provider_name::VARCHAR(255) AS provider_name,
  NULL::VARCHAR(20) AS npi,
  NULL::VARCHAR(20) AS dea,
  target_concept_id::BIGINT AS specialty_concept_id,
  c.care_site_id::BIGINT AS care_site_id,
  NULL::INTEGER AS year_of_birth,
  NULL::BIGINT AS gender_concept_id,
  provider_source_value::VARCHAR(50) AS provider_source_value,
  specialty_source_value::VARCHAR(50) AS specialty_source_value,
  NULL::BIGINT AS specialty_source_concept_id,
  NULL::VARCHAR(50) AS gender_source_value,
  NULL::BIGINT AS gender_source_concept_id,
  p.source_system::VARCHAR(20),
  p.org_code::VARCHAR(5)
FROM lth_bronze.vocab__provider AS p
LEFT JOIN (
  SELECT
    *
  FROM lth_bronze.care_site
  WHERE
    place_of_service_source_value = 'NHS Trust'
) AS c
  ON p.care_site_id::VARCHAR = c.care_site_source_value
