{{
  config(
    materialized = "table",
    tags = ['omop', 'era', 'drugs', 'lookup', 'dimension']
    )
}}

select
  row_number() over (
    partition by person_id,
    drug_concept_id,
    drug_sub_exposure_end_date order by person_id
  ) as row_number,
  person_id,
  drug_concept_id,
  MIN(drug_exposure_start_date) as drug_sub_exposure_start_date,
  drug_sub_exposure_end_date,
  COUNT(*) as drug_exposure_count
from lth_bronze.era__drug_exposure_ends 
group by person_id, drug_concept_id, drug_sub_exposure_end_date
--ORDER BY person_id, drug_concept_id
