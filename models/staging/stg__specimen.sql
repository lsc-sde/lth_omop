{{
  config(
    materialized = "view",
    tags = ['specimen', 'staging'],
    docs = {
        'name': 'stg__specimen',
        'description': 'Specimen view for all sources'
    }
    )
}}

with person as (
select
    *,
    row_number() over (partition by person_id order by last_edit_time desc) as id
from {{ ref('stg__master_patient_index') }}
)

select distinct
  mpi.person_id,
  mpi.person_source_value,
  sl_ba.nhs_number,
  sl_ba.order_date,
  sl_ba.site,
  qualifier,
  order_number,
  measurement_event_id,
  last_edit_time as updated_at
from {{ ref('stg_sl__bacteriology') }} as sl_ba
inner join person as mpi
  on sl_ba.nhs_number = cast(mpi.nhs_number as varchar)
  and id = 1
where
  sl_ba.site is not null
  and sl_ba.order_date is not null
