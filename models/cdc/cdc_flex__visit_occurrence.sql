{{
    config(
        materialized='view',
        tags = ['visit', 'cdc', 'flex']
        )
}}

with cdc as (
  select min(updated_at) as updated_at
  from {{ ref('cdc__updated_at') }}
  where
    model in ('VISIT_OCCURRENCE') and datasource = 'flex'
)

select *
from
  {{ ref('src_flex__visit_segment') }} as sfv
where
  sfv.last_edit_time > (
    select updated_at from cdc
  )
  and sfv.last_edit_time < (
    select dateadd(day, 90, updated_at) from cdc
  )
  and sfv.last_edit_time <= getdate()
