{{
  config(
    materialized = "view",
    tags = ['bi', 'flex', 'staging', 'provider']
    )
}}

select distinct
  efms.emp_provider_id,
  ms.name
from {{ ref('src_flex__emp_facility_med_spec') }} as efms
left join {{ ref('src_flex__medical_specialty') }} as ms
  on efms.physician_service_id = ms.physician_service_id
where item_nbr = 1
