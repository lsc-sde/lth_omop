
MODEL (
  name lth_bronze.cdc_flex__ae_diagnosis,
  kind full,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('CONDITION_OCCURRENCE') and datasource = 'flex_ae'
)

select
  visit_id,
  patient_id,
  diag_list,
  activation_time,
  admission_date_time,
  discharge_date_time,
  org_code::varchar(5),
  source_system::varchar(20),
  last_edit_time,
  updated_at
from
  lth_bronze.src_flex__ae_diagnosis as sfr
where
  sfr.last_edit_time > (
    select updated_at from cdc
  )
  and sfr.last_edit_time < (
    select dateadd(day, 10, updated_at) from cdc
  )
  and sfr.last_edit_time <= getdate()