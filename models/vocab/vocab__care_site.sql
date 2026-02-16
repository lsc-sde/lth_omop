MODEL (
  name vcb.vocab__care_site,
  kind FULL,
  cron '@daily'
);

WITH all_care_sites /*  union all */ /*  select */ /*    concat(40, dbo.IDGeneration(care_site_source_value)) as care_site_id, */ /*    care_site_name, */ /*    care_site_source_value, */ /*    postcode::varchar, */ /*    place_of_service_source_value, */ /*    location_source_value */ /*  from stg.stg_sl__care_site */ AS (
  SELECT
    care_site_id::BIGINT AS care_site_id,
    care_site_name,
    care_site_source_value,
    postcode,
    place_of_service_source_value,
    location_source_value,
    'external' AS source_system,
    'ods' AS org_code
  FROM stg.stg_ext__care_site
  UNION ALL
  SELECT
    care_site_id,
    care_site_name,
    care_site_source_value,
    postcode::VARCHAR,
    place_of_service_source_value,
    location_source_value,
    source_system,
    org_code
  FROM stg.stg_flex__care_site
)
SELECT
  care_site_id::BIGINT,
  care_site_name::VARCHAR(450),
  care_site_source_value::VARCHAR(450),
  postcode::VARCHAR(15),
  place_of_service_source_value::VARCHAR(450),
  location_source_value::VARCHAR(450),
  CASE
    WHEN (
      care_site_source_value LIKE '232~%'
      OR care_site_source_value LIKE '567~%'
      OR care_site_source_value LIKE '28~%'
      OR care_site_source_value IN ('28', '232', '567')
    )
    AND care_site_name LIKE '%GTD%'
    THEN '8782'
    WHEN (
      care_site_source_value LIKE '232~%'
      OR care_site_source_value LIKE '567~%'
      OR care_site_source_value LIKE '28~%'
      OR care_site_source_value IN ('28', '232', '567')
    )
    AND NOT care_site_name LIKE '%GTD%'
    THEN '8870'
    WHEN place_of_service_source_value = 'Quadramed Location'
    THEN '8717'
    WHEN place_of_service_source_value = 'Outpatient Clinic'
    THEN '8756'
    WHEN place_of_service_source_value = 'GP Practice'
    THEN NULL
    WHEN place_of_service_source_value = 'NHS Trust'
    THEN '8717'
  END::BIGINT AS place_of_service_concept_id,
  source_system::VARCHAR(20),
  org_code::VARCHAR(5)
FROM all_care_sites
