{{
  config(
    materialized = "table",
    tags = ['omop', 'drugs', 'era']
    )
}}

select
  cast(row_number() over (order by newid()) as bigint) as drug_era_id,
  cast(person_id as bigint) as person_id,
  cast(drug_concept_id as bigint) as drug_concept_id,
  min(drug_sub_exposure_start_date) as drug_era_start_date,
  drug_era_end_date,
  sum(drug_exposure_count) as drug_exposure_count,
  datediff(day, min(drug_sub_exposure_start_date), drug_era_end_date)
  - sum(days_exposed) as gap_days
from {{ ref('era__drug_era_ends') }}
group by person_id, drug_concept_id, drug_era_end_date;
