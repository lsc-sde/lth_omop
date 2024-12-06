{{
    config(
        materialized='view',
        tags = ['endoscopy', 'bulk', 'source', 'person']
    )
}}

select
  mrn,
  person_source_value,
  nhs_number
from {{ source('omop_source', 'src_gireport__person') }}
