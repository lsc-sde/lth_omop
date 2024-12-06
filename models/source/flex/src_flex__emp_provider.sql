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
from @catalaog_src.@schema_src.src_flex__emp_provider
