
MODEL (
  name lth_bronze.src_flex__implant_devices,
  kind FULL,
  cron '@daily',
);

select
  visit_id,
  patient_id,
  date_time,
  multi_field_occurrence_number,
  manufacturer,
  theatre_implants,
  sterilisation,
  expiry_date,
  ammendments,
  code_number,
  batch_lot_number
from @catalaog_src.@schema_src.src_flex__implant_devices

