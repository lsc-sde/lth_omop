{{
    config(
        materialized='table',
        tags = ['results', 'cdc', 'sl'],
        post_hook = [
          "{{ create_nonclustered_index(
                columns=['updated_at'],
                includes=['nhs_number'])
            }}"
        ]
    )
}}

with cdc as (
  select min(updated_at) as updated_at
  from {{ ref('cdc__updated_at') }}
  where
    model in ('MEASUREMENT', 'OBSERVATION') and datasource = 'swl'
)

select *
from
  {{ ref('stg_sl__bacteriology') }} as ssb
where
  ssb.updated_at > (
    select dateadd(d, -5, updated_at) from cdc
  )
  and ssb.updated_at <= getdate()
