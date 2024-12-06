{{
  config(
    materialized = "table",
    tags = ['bi', 'flex', 'staging', 'provider']
    )
}}

select distinct
  emp_provider_id,
  emp_type_id
from {{ ref('src_flex__emp_type_emp_facility') }}
