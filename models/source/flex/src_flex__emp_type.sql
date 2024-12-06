{{
  config(
    materialized = 'view',
    tags = ['provider', 'bulk', 'source', 'flex']
    )
}}

select
  emp_type_id,
  name
from @catalaog_src.@schema_src.src_flex__emp_type