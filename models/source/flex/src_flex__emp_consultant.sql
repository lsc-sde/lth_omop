{{
  config(
    materialized = 'view',
    tags = ['provider', 'bulk', 'source', 'flex']
    )
}}

select
cons_org_code,
cons_emp_provider_id,
cons_provider
from {{ source('omop_source', 'src_flex__emp_consultant') }}
