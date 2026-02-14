MODEL (
  name lth_bronze.src_flex__result,
  kind VIEW,
  cron '@daily'
);

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
  'rxn' AS org_code,
  'flex' AS source_system,
  last_edit_time,
  updated_at
FROM @catalog_src.@schema_src.src_flex__result AS sfr
