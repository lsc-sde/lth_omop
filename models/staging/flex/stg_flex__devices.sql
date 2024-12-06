{{
  config(
    materialized = "view",
    tags = ['flex', 'staging', 'devices'],
    docs = {
        'name': 'stg_flex__devices',
        'description': 'Devices staging table'
    }
    )
}}

with devices as
(
select
  visit_id,
  patient_id,
  date_time,
  manufacturer,
  'catheter' as device_type_group,
  cath_type as device_type,
  lot_number as device_lot_number,
  cath_details as device_details,
  null as expiry_date
from {{ ref('src_flex__cathether_devices') }}

union all

select
  visit_id,
  patient_id,
  date_time,
  manufacturer,
  theatre_implants as device_type_group,
  theatre_implants,
  batch_lot_number,
  ammendments,
  expiry_date
from {{ ref('src_flex__implant_devices') }}
),

visits as (
  select distinct
    visit_number,
    visit_id,
    first_visit_id,
    person_source_value
  from {{ ref('stg_flex__facility_transfer') }}
)

select
  isnull(v.first_visit_id, d.visit_id) as visit_id,
  patient_id,
  date_time,
  manufacturer,
  device_type_group,
  device_type,
  device_lot_number,
  device_details,
  expiry_date
from devices d
left join visits as v
  on d.visit_id = v.visit_id

