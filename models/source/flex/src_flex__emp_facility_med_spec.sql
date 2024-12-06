{{
  config(
    materialized = 'view',
    tags = ['provider', 'bulk', 'source', 'flex']
    )
}}

select
  emp_provider_id,
  facility_id,
  physician_service_id,
  item_nbr
from {{ source('omop_source', 'src_flex__emp_facility_med_spec') }}
