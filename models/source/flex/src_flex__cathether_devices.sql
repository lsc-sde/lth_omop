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
from @catalaog_src.@schema_src.src_flex__cathether_devices

