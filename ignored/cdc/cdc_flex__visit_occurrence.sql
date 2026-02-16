MODEL (
  name lth_bronze.cdc_flex__visit_occurrence,
  kind VIEW,
  cron '@daily',
  enabled (1 = 0)
);

WITH cdc AS (
  SELECT
    min(updated_at) AS updated_at
  FROM lth_bronze.cdc__updated_at
  WHERE
    model IN ('VISIT_OCCURRENCE') AND datasource = 'flex'
)
SELECT
  person_source_value,
  visit_id,
  visit_number,
  visit_type_id,
  visit_subtype_id,
  visit_status_id,
  admission_source,
  facility_id,
  attending_emp_provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  discharge_type_id,
  discharge_dest_code,
  discharge_dest_value,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  last_edit_time,
  updated_at
FROM lth_bronze.src_flex__visit_segment AS sfv
WHERE
  sfv.last_edit_time > (
    SELECT
      updated_at
    FROM cdc
  )
  AND sfv.last_edit_time < (
    SELECT
      dateadd(DAY, 365, updated_at)
    FROM cdc
  )
  AND sfv.last_edit_time <= getdate()
