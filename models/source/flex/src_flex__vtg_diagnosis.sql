{{
    config(
        materialized='view',
        tags = ['condition', 'bulk', 'source', 'flex']

)
}}

select
  visit_number,
  episode_id,
  index_nbr,
  icd10_code,
  provider_source_value,
  episode_start_dt,
  episode_end_dt,
  last_edit_time,
  updated_at
from @catalaog_src.@schema_src.src_flex__vtg_diagnosis
