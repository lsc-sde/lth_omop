{{
  config(
    materialized = 'view',
    tags = ['provider', 'bulk', 'source', 'flex']
    )
}}


select
  name,
  emp_provider_id,
  provider_id
from {{ source('omop_source', 'src_flex__emp_provider') }}
