MODEL (
  name stg.stg_flex__provider_specialty,
  kind FULL,
  cron '@daily'
);

SELECT DISTINCT
  efms.emp_provider_id,
  ms.name,
  efms.source_system::VARCHAR(20),
  efms.org_code::VARCHAR(5)
FROM src.src_flex__emp_facility_med_spec AS efms
LEFT JOIN src.src_flex__medical_specialty AS ms
  ON efms.physician_service_id = ms.physician_service_id
WHERE
  item_nbr = 1
