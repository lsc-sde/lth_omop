
MODEL (
  name src.src_bi__referrals,
  kind VIEW,
  cron '@daily',
);

select
  referral_received_date,
  patient_id,
  visit_id,
  referral_source,
  gp_priority,
  consultant_priority,
  consultant_code,
  referring_emp_code,
  priority,
  two_week_referral,
  suspected_cancer_type,
  treatment_function_code,
  treatment_function_name,
  'rxn' as org_code,
  'bi' as source_system,
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_bi__referrals
