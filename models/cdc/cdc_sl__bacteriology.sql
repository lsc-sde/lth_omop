MODEL (
  name lth_bronze.cdc_sl__bacteriology,
  kind SCD_TYPE_2_BY_TIME (
    unique_key measurement_event_id
  ),
  cron '@daily'
);

SELECT
  nhs_number,
  mrn,
  date_of_birth,
  visit_occurrence_id,
  isolate_event_id,
  measurement_event_id,
  order_date,
  provider_id,
  site,
  qualifier,
  order_number,
  source_name,
  source_code,
  value_source_value,
  source_value,
  value_as_number,
  unit_source_value,
  priority,
  org_code,
  source_system,
  updated_at,
  last_edit_time
FROM lth_bronze.stg_sl__bacteriology AS ssb
WHERE
  ssb.last_edit_time BETWEEN @start_ds AND @end_ds
