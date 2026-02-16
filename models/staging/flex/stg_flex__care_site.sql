MODEL (
  name stg.stg_flex__care_site,
  kind FULL,
  cron '@daily'
);

SELECT
  e.care_site_id,
  e.care_site_name::VARCHAR(255),
  e.care_site_source_value::VARCHAR(255),
  e.postcode::VARCHAR(10),
  e.place_of_service_source_value::VARCHAR(50),
  p.location_source_value::VARCHAR(255),
  source_system::VARCHAR(20),
  org_code::VARCHAR(5)
FROM (
  SELECT
    care_site_id::NUMERIC::BIGINT AS care_site_id,
    concat_ws(
      ' ',
      area,
      CASE WHEN subarea = 'NA' THEN NULL ELSE subarea END,
      CASE WHEN sub_subarea = 'NA' THEN NULL ELSE sub_subarea END
    ) AS care_site_name,
    location_id AS care_site_source_value,
    CASE
      WHEN facility = 'Royal Preston Hospital'
      THEN '1'
      WHEN facility = 'Chorley and South Ribble District Hospital'
      THEN '3'
    END AS postcode,
    'Quadramed Location' AS place_of_service_source_value,
    source_system,
    org_code
  FROM src.src_flex__care_site_ip
  UNION
  SELECT DISTINCT
    care_site_id::NUMERIC::BIGINT AS care_site_id,
    care_site_name,
    care_site_source_value,
    location_id AS postcode,
    'Outpatient Clinic' AS place_of_service_source_value,
    source_system,
    org_code
  FROM src.src_flex__care_site_op
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
