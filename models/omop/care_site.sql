MODEL (
  name cdm.care_site,
  kind FULL,
  cron '@daily'
);

SELECT DISTINCT
  care_site_id::BIGINT AS care_site_id,
  care_site_name::VARCHAR(255) AS care_site_name,
  place_of_service_concept_id::BIGINT AS place_of_service_concept_id,
  location_id::BIGINT AS location_id,
  care_site_source_value::VARCHAR(50) AS care_site_source_value,
  place_of_service_source_value::VARCHAR(50) AS place_of_service_source_value,
  cs.source_system::VARCHAR(20),
  cs.org_code::VARCHAR(5)
FROM vcb.vocab__care_site AS cs
LEFT JOIN (
  SELECT
    min(location_id) AS location_id,
    location_source_value AS location_source_value
  FROM cdm.location
  GROUP BY
    location_source_value
) AS l
  ON cs.location_source_value = l.location_source_value
