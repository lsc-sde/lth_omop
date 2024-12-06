{{
  config(
    materialized = "table",
    tags = ['omop', 'era', 'drugs', 'lookup', 'dimension']
    )
}}

select
  row_number,
  person_id,
  drug_concept_id as ingredient_concept_id,
  drug_sub_exposure_start_date,
  drug_sub_exposure_end_date,
  drug_exposure_count,
  datediff(day, drug_sub_exposure_start_date, drug_sub_exposure_end_date)
    as days_exposed
from lth_bronze.era__drug_sub_exposures 
