
MODEL (
  name lth_bronze.cdc_sl__bacteriology,
  kind FULL,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at 
  where
    model in ('MEASUREMENT', 'OBSERVATION') and datasource = 'swl'
)

select *
from
  lth_bronze.stg_sl__bacteriology as ssb
where
  ssb.updated_at > (
    select dateadd(d, -5, updated_at) from cdc
  )
  and ssb.updated_at <= getdate()
