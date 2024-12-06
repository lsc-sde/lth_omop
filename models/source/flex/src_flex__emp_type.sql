{{
  config(
    materialized = 'view',
    tags = ['provider', 'bulk', 'source', 'flex']
    )
}}

select
  emp_type_id,
  name
from {{ source('omop_source', 'src_flex__emp_type') }}