
MODEL (
  name lth_bronze.era__drug_final_target,
  kind FULL,
  cron '@daily',
);

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
