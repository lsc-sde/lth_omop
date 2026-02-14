MODEL (
  name lth_bronze.cdc_flex__result,
  kind FULL,
  cron '@daily',
  enabled (
    1 = 0
  )
);

WITH cdc AS (
  SELECT
    min(updated_at) AS updated_at
  FROM lth_bronze.cdc__updated_at
  WHERE
    model IN ('MEASUREMENT', 'OBSERVATION') AND datasource = 'flex'
)
SELECT
  result_id,
  visit_id,
  event_id,
  person_source_value,
  event_date_time,
  data_element_id,
  procedure_name,
  kardex_group_id,
  field_name,
  result_value,
  trimmed_result_value,
  display_unit,
  event_status_id,
  emp_provider_id,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  last_edit_time,
  updated_at
FROM lth_bronze.src_flex__result AS sfr
WHERE
  sfr.last_edit_time > (
    SELECT
      updated_at
    FROM cdc
  )
  AND sfr.last_edit_time < (
    SELECT
      dateadd(DAY, 365, updated_at)
    FROM cdc
  )
  AND sfr.last_edit_time <= getdate()
