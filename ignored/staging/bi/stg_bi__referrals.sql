
MODEL (
  name lth_bronze.stg_bi__referrals,
  kind FULL,
  cron '@daily',
);

select
  bi.patient_id as bi_patient_id,
  bi.visit_id as visit_occurrence_id,
  null as measurement_event_id,
  bi.referral_received_date,
  pr.provider_id,
  bi.treatment_function_code as source_code,
  bi.treatment_function_name as source_name,
  null as value_source_value,
  null as source_value,
  null as value_as_number,
  null as unit_source_value,
  coalesce(
    bi.suspected_cancer_type,
    bi.consultant_priority,
    bi.gp_priority)
    as priority,
  bi.last_edit_time,
  bi.updated_at
from lth_bronze.cdc_bi__referrals as bi
left join lth_bronze.stg__provider as pr
  on bi.referring_emp_code = pr.provider_source_value
