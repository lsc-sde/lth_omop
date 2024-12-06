{{
    config(
        materialized='view',
        tags = ['endoscopy', 'bulk', 'source']
    )
}}

select
  person_source_value,
  visit_occurrence_id,
  anatomic_site_source_value,
  parent_procedure_source_value,
  procedure_source_value
from @catalaog_src.@schema_src.src_gireport__lowergi_therapeutic