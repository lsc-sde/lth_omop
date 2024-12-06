{{
  config(
    materialized = "table",
    tags = ['scr', 'staging', 'condition']
    )
}}


select
  mrn,
  nhs_number,
  tumour_status,
  date_of_diagnosis,
  icd_primary_diagnosis,
  secondary_diagnosis,
  isnull(
    left(secondary_diagnosis, charindex('-', secondary_diagnosis) - 2),
    left(icd_primary_diagnosis, charindex('-', icd_primary_diagnosis) - 2)
  ) as icd_code,
  laterality,
  basis_of_diagnosis,
  last_edit_time,
  updated_at
from lth_bronze.src_scr__diagnosis 
where icd_primary_diagnosis is not null
