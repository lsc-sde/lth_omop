{{
    config(
        materialized='view',
        tags = ['procedure', 'bulk', 'source', 'flex']

)
}}

select
  visit_number,
  episode_id,
  index_nbr,
  opcs4_code,
  provider_source_value,
  procedure_date,
  last_edit_time,
  updated_at
from @catalaog_src.@schema_src.src_flex__vtg_procedure
