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
from {{ source('omop_source', 'src_gireport__uppergi_diagnosis') }}