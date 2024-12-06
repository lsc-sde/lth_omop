{{
  config(
    materialized = 'view',
    tags = ['provider', 'bulk', 'source', 'flex']
    )
}}


select
  emp_type_id,
  emp_provider_id,
  facility_id
from {{ source('omop_source', 'src_flex__emp_type_emp_facility') }}
