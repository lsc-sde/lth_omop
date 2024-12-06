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
from {{ source('omop_source', 'src_gireport__diagnostic_codes') }}
