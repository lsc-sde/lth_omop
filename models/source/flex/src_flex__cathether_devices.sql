
MODEL (
  name lth_bronze.src_flex__cathether_devices,
  kind VIEW,
  cron '@daily',
);

select
  visit_id,
  patient_id,
  date_time,
  manufacturer,
  cath_type,
  lot_number,
  cath_details
from @catalog_src.@schema_src.src_flex__cathether_devices

