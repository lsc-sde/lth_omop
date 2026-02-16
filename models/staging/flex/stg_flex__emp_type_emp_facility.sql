MODEL (
  name stg.stg_flex__emp_type_emp_facility,
  kind FULL,
  cron '@daily'
);

SELECT DISTINCT
  emp_provider_id,
  emp_type_id,
  source_system::VARCHAR(20),
  org_code::VARCHAR(5)
FROM src.src_flex__emp_type_emp_facility
