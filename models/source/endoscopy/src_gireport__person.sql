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
from @catalaog_src.@schema_src.src_gireport__person
