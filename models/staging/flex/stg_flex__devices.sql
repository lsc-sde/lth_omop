
MODEL (
  name lth_bronze.stg_flex__devices,
  kind FULL,
  cron '@daily',
);

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
from lth_bronze.src_flex__cathether_devices 

union all

select
  visit_id,
  patient_id,
  date_time,
  manufacturer,
  theatre_implants as device_type_group,
  theatre_implants as device_type,
  batch_lot_number as device_lot_number,
  ammendments as device_details,
  expiry_date
from lth_bronze.src_flex__implant_devices 
),

visits as (
  select distinct
    visit_number,
    visit_id,
    first_visit_id,
    person_source_value
  from lth_bronze.stg_flex__facility_transfer 
)

select
  isnull(v.first_visit_id, d.visit_id)::BIGINT as visit_id,
  patient_id::BIGINT,
  date_time::DATETIME,
  manufacturer::VARCHAR(MAX),
  device_type_group::VARCHAR(MAX),
  device_type::VARCHAR(MAX),
  device_lot_number::VARCHAR(MAX),
  device_details::VARCHAR(MAX),
  expiry_date::DATETIME
from devices d
left join visits as v
  on d.visit_id = v.visit_id

