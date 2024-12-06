{{
    config(
        materialized='view',
        tags = ['endoscopy', 'bulk', 'source', 'condition']
    )
}}

select
  person_source_value,
  visit_occurrence_id,
  condition_source_value
from @catalaog_src.@schema_src.src_gireport__condition_occurrence