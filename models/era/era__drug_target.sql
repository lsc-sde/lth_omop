
MODEL (
  name lth_bronze.era__drug_target,
  kind FULL,
  cron '@daily',
);

select
  d.drug_exposure_id,
  d.person_id,
  c.concept_id as ingredient_concept_id,
  d.drug_exposure_start_date as drug_exposure_start_date,
  d.days_supply as days_supply,
  coalesce(
    nullif(drug_exposure_end_date, null),
    nullif(
      dateadd(day, days_supply, drug_exposure_start_date),
      drug_exposure_start_date
    ),
    dateadd(day, 1, drug_exposure_start_date)
  ) as drug_exposure_end_date
from lth_bronze.DRUG_EXPOSURE as d
inner join
  vocab.concept_ancestor as ca
  on ca.descendant_concept_id = d.drug_concept_id
inner join vocab.concept as c on ca.ancestor_concept_id = c.concept_id
where
  c.vocabulary_id = 'RxNorm'
  and c.concept_class_id = 'Ingredient'
  and d.drug_concept_id != 0
  and coalesce(d.days_supply, 0) >= 0
