{{
    config(
        materialized='view',
        tags = ['endoscopy', 'bulk', 'source', 'procedure']
    )
}}

select
  person_source_value,
  visit_occurrence_id,
  care_site_id,
  procedure_date,
  procedure_datetime,
  provider_id,
  procedure_concept_id,
  procedure_source_value
from {{ source('omop_source', 'src_gireport__lowergi_procedure') }}
