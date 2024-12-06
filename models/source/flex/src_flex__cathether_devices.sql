{{
  config(
    materialized = 'view'
    )
}}

select
  visit_id,
  patient_id,
  date_time,
  manufacturer,
  cath_type,
  lot_number,
  cath_details
from {{ source('omop_source', 'src_flex__cathether_devices') }}

