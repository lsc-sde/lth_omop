{{
    config(
        materialized='table',
        tags = ['person', 'bulk', 'source', 'flex']
)
}}

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
from {{ source('omop_source', 'src_flex__person') }}
