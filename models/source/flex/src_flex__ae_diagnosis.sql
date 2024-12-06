{{
    config(
        materialized='view',
        tags = ['condition', 'bulk', 'source', 'ae', 'flex']

)
}}

select
  visit_id,
  patient_id,
  diag_list,
  activation_time,
  admission_date_time,
  discharge_date_time,
  last_edit_time,
  updated_at
from {{ source('omop_source', 'src_flex__ae_diagnosis') }}
