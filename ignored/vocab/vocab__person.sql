
MODEL (
  name lth_bronze.vocab__person,
  kind FULL,
  cron '@daily',
);

select
  person_id,
  provider_id,
  gp_prac_code,
  birth_datetime,
  0 as ethnicity_concept_id, -- unknown ethnicity code
  mailing_code,
  person_source_value,
  gender_source_value,
  race_source_value,
  case
    when gender_source_value like '%Unknown%' then '8551'
    else v.target_concept_id
  end as gender_concept_id,
  isnull(
    case
      when v2.target_concept_id in (46273465, 35607960) then 0
      else v2.target_concept_id
    end,
    0
  ) as race_concept_id
from lth_bronze.stg__person as p
left join
  (
    select
      target_concept_id,
      source_code
    from lth_bronze.vocab__source_to_concept_map 
    where [group] = 'demographics'
  ) as v
  on p.gender_source_value = v.source_code
left join
  (
    select
      target_concept_id,
      source_code,
      source_code_description
    from lth_bronze.vocab__source_to_concept_map 
    where [group] = 'demographics'
  ) as v2
  on cast(p.race_source_value as varchar) = v2.source_code_description
