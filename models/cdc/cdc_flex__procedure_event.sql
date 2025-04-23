
MODEL (
  name lth_bronze.cdc_flex__procedure_event,
  kind full,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('PROCEDURE_OCCURRENCE') and datasource = 'flex'
)

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
  org_code::varchar(5),
  source_system::varchar(20),
  last_edit_time,
  updated_at
from 
  lth_bronze.src_flex__procedure_event sfr
where
  sfr.last_edit_time > (
    select updated_at from cdc
  )
  and sfr.last_edit_time < (
    select dateadd(day, 10, updated_at) from cdc
  )
  and sfr.last_edit_time <= getdate()