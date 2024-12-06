{{
  config(
    materialized = 'view',
    tags = ['provider', 'bulk', 'source', 'flex']
    )
}}

select
  physician_service_id,
  name,
  facility_id,
  parent_physician_service_id
from {{ source('omop_source', 'src_flex__medical_specialty') }}