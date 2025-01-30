
MODEL (
  name lth_bronze.src_flex__visit_detail_ae,
  kind VIEW,
  cron '@daily',
);

select
  patient_id,
  visit_id,
  visit_number,
  visit_segment_number,
  visit_type_id,
  visit_subtype_id,
  visit_status_id,
  facility_id,
  location_id,
  service_type,
  admitting_emp_provider_id,
  date_time_in,
  date_time_out,
  activation_time,
  'rxn' as org_code,
  'flex' as source_system,
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_flex__visit_detail_ae