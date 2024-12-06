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
from @catalaog_src.@schema_src.src_flex__ae_procedures
