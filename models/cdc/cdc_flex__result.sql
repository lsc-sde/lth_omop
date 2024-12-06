{{
    config(
        materialized='view',
        tags = ['results', 'cdc', 'flex']
        )
}}

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at 
  where
    model in ('MEASUREMENT', 'OBSERVATION') and datasource = 'flex'
)

select *
from
  lth_bronze.src_flex__result as sfr
where
  sfr.last_edit_time > (
    select updated_at from cdc
  )
  and sfr.last_edit_time < (
    select dateadd(day, 90, updated_at) from cdc
  )
  and sfr.last_edit_time <= getdate()
