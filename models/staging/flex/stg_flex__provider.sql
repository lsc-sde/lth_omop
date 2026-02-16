MODEL (
  name stg.stg_flex__provider,
  kind FULL,
  cron '@daily'
);

WITH provider_base AS (
  SELECT DISTINCT
    ep.name AS provider_name,
    2 AS care_site_id,
    ep.emp_provider_id AS provider_source_value,
    coalesce(emp_med_spec.name, et.name) AS specialty_source_value,
    row_number() OVER (
      PARTITION BY ep.emp_provider_id
      ORDER BY CASE
        WHEN coalesce(emp_med_spec.name, et.name) LIKE '%Nurse%'
        OR coalesce(emp_med_spec.name, et.name) LIKE '%Consultant%'
        OR coalesce(emp_med_spec.name, et.name) LIKE '%Pharmacist%'
        OR coalesce(emp_med_spec.name, et.name) LIKE '%Radiographer%'
        THEN 1
        ELSE 0
      END DESC
    ) AS identifier,
    ep.source_system AS source_system,
    ep.org_code AS org_code
  FROM src.src_flex__emp_provider AS ep
  LEFT JOIN stg.stg_flex__provider_specialty AS emp_med_spec
    ON ep.emp_provider_id = emp_med_spec.emp_provider_id
  LEFT JOIN stg.stg_flex__emp_type_emp_facility AS e
    ON ep.emp_provider_id = e.emp_provider_id
  LEFT JOIN src.src_flex__emp_type AS et
    ON e.emp_type_id = et.emp_type_id
)
SELECT
  provider_name,
  care_site_id,
  provider_source_value::VARCHAR AS provider_source_value,
  specialty_source_value,
  c.cons_org_code,
  row_number() OVER (ORDER BY NEWID()) AS provider_id,
  p.source_system::VARCHAR(20),
  p.org_code::VARCHAR(5)
FROM provider_base AS p
LEFT JOIN src.src_flex__emp_consultant AS c
  ON p.provider_source_value = c.cons_emp_provider_id
WHERE
  p.identifier = 1
