
MODEL (
  name lth_bronze.cdc_bi__referrals,
  kind view,
  cron '@daily',
  enabled false
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('OBSERVATION') and datasource = 'bi'
)

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
  org_code::varchar(5),
  source_system::varchar(20),
  last_edit_time,
  updated_at
from
  lth_bronze.src_bi__referrals as sbr
where
  sbr.updated_at > (
    select updated_at from cdc
  )
  and sbr.updated_at <= getdate()
