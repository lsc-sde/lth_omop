{{
    config(
        materialized='view',
        tags = ['visit', 'bulk', 'visit_detail', 'ae', 'source', 'flex']
    )
}}

select
  visit_id,
  visit_number,
  patient_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  location_id,
  location_hx_time,
  last_edit_time,
  updated_at
from @catalaog_src.@schema_src.src_flex__visit_detail_ip
