{{
  config(
    materialized = "table",
    tags = ['omop', 'era', 'drugs', 'lookup', 'dimension']
    )
}}

select
  ft.person_id,
  ft.ingredient_concept_id as drug_concept_id,
  ft.drug_sub_exposure_start_date,
  MIN(e.end_date) as drug_era_end_date,
  drug_exposure_count,
  days_exposed
from lth_bronze.era__drug_final_target as ft
inner join
  lth_bronze.era__drug_end_dates as e
  on
    ft.person_id = e.person_id
    and ft.ingredient_concept_id = e.ingredient_concept_id
    and e.end_date >= ft.drug_sub_exposure_start_date
group by
  ft.person_id,
  ft.ingredient_concept_id,
  ft.drug_sub_exposure_start_date,
  drug_exposure_count,
  days_exposed
