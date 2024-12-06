
MODEL (
  name lth_bronze.src_flex__ae_diagnosis,
  kind FULL,
  cron '@daily',
);

select
  visit_id,
  patient_id,
  diag_list,
  activation_time,
  admission_date_time,
  discharge_date_time,
  last_edit_time,
  updated_at
from @catalaog_src.@schema_src.src_flex__ae_diagnosis
