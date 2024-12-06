
MODEL (
  name lth_bronze.src_flex__procedure_event,
  kind FULL,
  cron '@daily',
);

with source_data as (
select
  *
from @catalaog_src.@schema_src.src_flex__procedure_event
)

{% if is_incremental() %}

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
  last_edit_time,
  updated_at
from source_data
where last_edit_time > (select max(last_edit_time) from {{ this }})

{% else %}

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
  last_edit_time,
  updated_at
from source_data

{% endif %}
