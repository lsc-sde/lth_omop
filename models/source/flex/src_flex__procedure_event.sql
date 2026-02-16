
MODEL (
  name src.src_flex__procedure_event,
  kind VIEW,
  cron '@daily',
);

select
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
  'rxn' as org_code,
  'flex' as source_system,
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_flex__procedure_event
