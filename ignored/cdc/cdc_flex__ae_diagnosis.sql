MODEL (
  name lth_bronze.cdc_flex__ae_diagnosis,
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
    model IN ('CONDITION_OCCURRENCE') AND datasource = 'flex_ae'
)
SELECT
  visit_id,
  patient_id,
  diag_list,
  activation_time,
  admission_date_time,
  discharge_date_time,
  org_code::VARCHAR(5),
  'flex_ae'::VARCHAR(20) AS source_system,
  last_edit_time,
  updated_at
FROM lth_bronze.src_flex__ae_diagnosis AS sfr
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
