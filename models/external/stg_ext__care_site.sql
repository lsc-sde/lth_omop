MODEL (
  name stg.stg_ext__care_site,
  kind FULL,
  cron '@daily'
);

SELECT
  e.care_site_id,
  e.care_site_name::VARCHAR(255),
  e.care_site_source_value::VARCHAR(255),
  e.postcode::VARCHAR(10),
  e.place_of_service_source_value::VARCHAR(50),
  p.location_source_value::VARCHAR(15)
FROM (
  SELECT DISTINCT
    care_site_id::NUMERIC::BIGINT AS care_site_id,
    care_site_name,
    care_site_source_value,
    care_site_location AS postcode,
    'GP Practice' AS place_of_service_source_value
  FROM src.src_flex__care_site_gp
  UNION
  SELECT
    10,
    'Lancashire Teaching Hospitals',
    '2',
    'PR2 9HT' AS postcode,
    'NHS Trust' AS place_of_service_source_value
  UNION
  SELECT
    11,
    'Royal Preston Hospital',
    '1',
    'PR2 9HT' AS postcode,
    'NHS Trust' AS place_of_service_source_value
  UNION
  SELECT
    12,
    'Chorley District Hospital',
    '3',
    'PR7 1PP' AS postcode,
    'NHS Trust' AS place_of_service_source_value
) AS e
LEFT JOIN ext.ext__postcodes AS p
  ON CASE
    WHEN e.postcode = '1'
    THEN 'PR29HT'
    WHEN e.postcode = '3'
    THEN 'PR71PP'
    ELSE replace(e.postcode, ' ', '')
  END = replace(p.postcode, ' ', '')
WHERE
  NOT e.postcode IS NULL
