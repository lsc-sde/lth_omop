{{
    config(
        materialized='view',
        tags = ['bi', 'flex', 'staging', 'condition']
    )
}}

select
  visit_id,
  patient_id,
  126 as provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  last_edit_time,
  updated_at,
  value as diagnosis_list
from (
  select
    visit_id,
    patient_id,
    activation_time,
    admission_date_time,
    discharge_date_time,
    last_edit_time,
    updated_at,
    value as diag_list
  from lth_bronze.src_flex__ae_diagnosis 
  cross apply string_split(diag_list, '~')
) as t
cross apply string_split(diag_list, '|')
where
  len(value) > 1
  and value not in ('410605003')
