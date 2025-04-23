
MODEL (
  name lth_bronze.src_flex__visit_detail_ip,
  kind VIEW,
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
  'rxn' as org_code,
  'flex' as source_system,
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_flex__visit_detail_ip
