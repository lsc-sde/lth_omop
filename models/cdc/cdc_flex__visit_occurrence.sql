
MODEL (
  name lth_bronze.cdc_flex__visit_occurrence,
  kind view,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('VISIT_OCCURRENCE') and datasource = 'flex'
)

select
  person_source_value,
  visit_id,
  visit_number,
  visit_type_id,
  visit_subtype_id,
  visit_status_id,
  admission_source,
  facility_id,
  attending_emp_provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  discharge_type_id,
  discharge_dest_code,
  discharge_dest_value,
  org_code::varchar(5),
  source_system::varchar(20),
  last_edit_time,
  updated_at
from
  lth_bronze.src_flex__visit_segment as sfv
where
  sfv.last_edit_time > (
    select updated_at from cdc
  )
  and sfv.last_edit_time < (
    select dateadd(day, 365, updated_at) from cdc
  )
  and sfv.last_edit_time <= getdate()