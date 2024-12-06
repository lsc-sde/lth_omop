{{
    config(
        materialized = 'table',
        tags = ['bulk', 'source', 'sl', 'measurement']
    )
}}

select
  order_number,
  nhs_number,
  mrn,
  date_of_birth,
  postcode,
  sex,
  isolate_number,
  visit_number,
  location_code,
  clinician,
  clinician_code,
  order_date,
  result_date,
  site,
  qualifier,
  test,
  result_value,
  updated_at
from {{ source('omop_source', 'src_sl__bacteriology_archive') }}