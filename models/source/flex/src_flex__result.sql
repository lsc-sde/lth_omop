
MODEL (
  name lth_bronze.src_flex__result,
  kind VIEW,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at 
  where
    model in ('MEASUREMENT', 'OBSERVATION') and datasource = 'flex'
)

select
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
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_flex__result sfr
where
  sfr.last_edit_time > (
    select updated_at from cdc
  )
  and sfr.last_edit_time < (
    select dateadd(day, 90, updated_at) from cdc
  )
  and sfr.last_edit_time <= getdate()