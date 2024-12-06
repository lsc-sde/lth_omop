
MODEL (
  name lth_bronze.cdc_flex__visit_occurrence,
  kind FULL,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at 
  where
    model in ('VISIT_OCCURRENCE') and datasource = 'flex'
)

select *
from
  lth_bronze.src_flex__visit_segment as sfv
where
  sfv.last_edit_time > (
    select updated_at from cdc
  )
  and sfv.last_edit_time < (
    select dateadd(day, 90, updated_at) from cdc
  )
  and sfv.last_edit_time <= getdate()
