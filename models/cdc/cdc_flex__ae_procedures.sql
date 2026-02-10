
MODEL (
  name lth_bronze.cdc_flex__ae_procedures,
  kind full,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('PROCEDURE_OCCURRENCE') and datasource = 'flex_ae'
)

select
  visit_id,
  patient_id,
  list,
  activation_time,
  admission_date_time,
  discharge_date_time,
  org_code::varchar(5),
  'flex_ae'::varchar(20) as source_system,
  last_edit_time,
  updated_at
from
  lth_bronze.src_flex__ae_procedures as sfr
where
  sfr.last_edit_time > (
    select updated_at from cdc
  )
  and sfr.last_edit_time < (
    select dateadd(day, 365, updated_at) from cdc
  )
  and sfr.last_edit_time <= getdate()