
MODEL (
  name lth_bronze.cdc_flex__vtg_procedure,
  kind full,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('PROCEDURE_OCCURRENCE') and datasource = 'ukcoder'
)

select
  visit_number,
  episode_id,
  index_nbr,
  opcs4_code,
  provider_source_value,
  procedure_date,
  org_code::varchar(5),
  source_system::varchar(20),
  last_edit_time,
  updated_at
from
  lth_bronze.src_flex__vtg_procedure as sfr
where
  sfr.last_edit_time > (
    select updated_at from cdc
  )
  and sfr.last_edit_time < (
    select dateadd(day, 10, updated_at) from cdc
  )
  and sfr.last_edit_time <= getdate()