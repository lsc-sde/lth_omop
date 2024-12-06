{{
    config(
        materialized='view',
        tags = ['condition', 'bulk', 'staging', 'flex']
    )
}}

select
  index_nbr,
  icd10_code,
  provider_source_value,
  last_edit_time,
  replace(
    substring(
      visit_number,
      4,
      len(visit_number) - patindex('%[0-9] %', reverse(visit_number)) - 4
    ),
    ' ',
    '-'
  ) as visit_number
from
  lth_bronze.src_flex__vtg_diagnosis 
