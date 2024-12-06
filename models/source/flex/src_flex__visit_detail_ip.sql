
MODEL (
  name lth_bronze.src_flex__visit_detail_ip,
  kind FULL,
  cron '@daily',
);

select
  visit_id,
  visit_number,
  patient_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  location_id,
  location_hx_time,
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_flex__visit_detail_ip
