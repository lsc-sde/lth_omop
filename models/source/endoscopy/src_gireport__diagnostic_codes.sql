{{
    config(
        materialized='view',
        tags = ['endoscopy', 'bulk', 'source', 'condition']
    )
}}

select
  table_name,
  field_name,
  condition_source_value
from @catalaog_src.@schema_src.src_gireport__diagnostic_codes
