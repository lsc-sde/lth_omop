
MODEL (
  name lth_bronze.src_flex__person,
  kind FULL,
  cron '@daily',
);

select
  person_source_value,
  gender_source_value,
  race_source_value,
  mailing_code,
  collapsed_into_patient_id,
  provider_id,
  gp_prac_code,
  mrn,
  nhs_number,
  mother_patient_id,
  birth_datetime,
  death_datetime,
  last_edit_time,
  updated_at
from @catalaog_src.@schema_src.src_flex__person
