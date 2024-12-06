{{
  config(
    materialized = 'view'
    )
}}

select
  visit_id,
  patient_id,
  date_time,
  multi_field_occurrence_number,
  manufacturer,
  theatre_implants,
  sterilisation,
  expiry_date,
  ammendments,
  code_number,
  batch_lot_number
from {{ source('omop_source', 'src_flex__implant_devices') }}

