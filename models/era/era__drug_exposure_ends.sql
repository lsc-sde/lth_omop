{{
  config(
    materialized = "table",
    tags = ['omop', 'era', 'drugs', 'lookup', 'dimension']
    )
}}

select
  dt.person_id,
  dt.ingredient_concept_id as drug_concept_id,
  dt.drug_exposure_start_date,
  min(e.end_date) as drug_sub_exposure_end_date
from lth_bronze.era__drug_pre_target as dt
inner join
  lth_bronze.era__drug_sub_exposure_end_dates as e
  on
    dt.person_id = e.person_id
    and dt.ingredient_concept_id = e.ingredient_concept_id
    and e.end_date >= dt.drug_exposure_start_date
group by
  dt.drug_exposure_id,
  dt.person_id,
  dt.ingredient_concept_id,
  dt.drug_exposure_start_date
