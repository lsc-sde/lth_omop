MODEL (
  name lth_bronze.cdc_flex__vtg_procedure,
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
    model IN ('PROCEDURE_OCCURRENCE') AND datasource = 'ukcoder'
)
SELECT
  visit_number,
  episode_id,
  index_nbr,
  opcs4_code,
  provider_source_value,
  procedure_date,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  last_edit_time,
  updated_at
FROM lth_bronze.src_flex__vtg_procedure AS sfr
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
