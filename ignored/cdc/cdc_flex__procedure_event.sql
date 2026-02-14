MODEL (
  name lth_bronze.cdc_flex__procedure_event,
  kind FULL,
  cron '@daily',
  enabled (1 = 0)
);

WITH cdc AS (
  SELECT
    min(updated_at) AS updated_at
  FROM lth_bronze.cdc__updated_at
  WHERE
    model IN ('PROCEDURE_OCCURRENCE') AND datasource = 'flex'
)
SELECT
  visit_id,
  event_id,
  order_span_id,
  order_span_state_id,
  person_source_value,
  provider_source_value,
  flex_procedure_id,
  flex_procedure_name,
  kardex_group_id,
  device_id,
  dosage,
  adm_route,
  prn_dosage,
  event_status_id,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  last_edit_time,
  updated_at
FROM lth_bronze.src_flex__procedure_event AS sfr
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
