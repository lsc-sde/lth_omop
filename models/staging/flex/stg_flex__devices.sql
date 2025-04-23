
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
  null as expiry_date,
  source_system::varchar(20),
  org_code::varchar(5)
from lth_bronze.src_flex__cathether_devices

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
  expiry_date,
  source_system::varchar(20),
  org_code::varchar(5)
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
  isnull(v.first_visit_id, d.visit_id) as visit_id,
  patient_id,
  date_time,
  manufacturer::varchar(150),
  device_type_group::varchar(100),
  device_type::varchar(150),
  device_lot_number::varchar(100),
  device_details::varchar(450),
  expiry_date::varchar(20),
  source_system::varchar(20),
  org_code::varchar(5)
from devices d
left join visits as v
  on d.visit_id = v.visit_id
