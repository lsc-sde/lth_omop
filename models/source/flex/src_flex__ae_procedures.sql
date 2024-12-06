{{
    config(
        materialized='view',
        tags = ['procedure', 'bulk', 'source', 'ae', 'flex']

)
}}

select
  visit_id,
  patient_id,
  list,
  activation_time,
  admission_date_time,
  discharge_date_time,
  last_edit_time,
  updated_at
from {{ source('omop_source', 'src_flex__ae_procedures') }}
