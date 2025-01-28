
MODEL (
  name lth_bronze.src_flex__ae_diagnosis,
  kind VIEW,
  cron '@daily',
);

select
  visit_id,
  patient_id,
  diag_list,
  activation_time,
  admission_date_time,
  discharge_date_time,
  'rxn' as org_code,
  'flex' as source_system,
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_flex__ae_diagnosis
