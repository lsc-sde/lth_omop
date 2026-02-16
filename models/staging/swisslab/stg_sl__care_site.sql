MODEL (
  name stg.stg_sl__care_site,
  kind FULL,
  cron '@daily'
);

SELECT
  e.care_site_name,
  e.care_site_source_value,
  e.postcode,
  e.place_of_service_source_value,
  p.location_source_value,
  source_system,
  org_code
FROM (
  SELECT DISTINCT
    description AS care_site_name,
    location_code AS care_site_source_value,
    NULL AS postcode,
    'Swisslab' AS place_of_service_source_value,
    'swisslab' AS source_system,
    'rxn' AS org_code
  FROM lth_bronze.swisslab__locations
  WHERE
    quadramed_code IS NULL
) AS e
LEFT JOIN ext.ext__postcodes AS p
  ON CASE
    WHEN e.postcode = '1'
    THEN 'PR29HT'
    WHEN e.postcode = '3'
    THEN 'PR71PP'
    ELSE replace(e.postcode, ' ', '')
  END = replace(p.postcode, ' ', '')
