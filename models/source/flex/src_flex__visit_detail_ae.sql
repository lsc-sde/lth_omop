{{
    config(
        materialized='view',
        tags = ['visit', 'bulk', 'visit_detail', 'ae', 'source', 'flex']
    )
}}

select
  patient_id,
  visit_id,
  visit_number,
  visit_segment_number,
  visit_type_id,
  visit_subtype_id,
  visit_status_id,
  facility_id,
  location_id,
  service_type,
  admitting_emp_provider_id,
  date_time_in,
  date_time_out,
  activation_time,
  last_edit_time,
  updated_at
from @catalaog_src.@schema_src.src_flex__visit_detail_ae