
MODEL (
  name lth_bronze.cdc_flex__vtg_diagnosis,
  kind full,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('CONDITION_OCCURRENCE') and datasource = 'ukcoder'
)

select
  visit_number,
  episode_id,
  index_nbr,
  icd10_code,
  provider_source_value,
  episode_start_dt,
  episode_end_dt,
  org_code::varchar(5),
  source_system::varchar(20),
  last_edit_time,
  updated_at
from
  lth_bronze.src_flex__vtg_diagnosis as sfr
where
  sfr.last_edit_time > (
    select updated_at from cdc
  )
  and sfr.last_edit_time < (
    select dateadd(day, 365, updated_at) from cdc
  )
  and sfr.last_edit_time <= getdate()