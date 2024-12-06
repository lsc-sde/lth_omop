{{
    config(
        tags = ['vocab', 'drugs']
    )
}}

select
  visit_occurrence_id,
  person_id,
  c_cm.target_concept_id,
  drug_exposure_start_date,
  drug_exposure_start_datetime,
  provider_id,
  32838 as drug_type_concept_id,
  r_cm.target_concept_id as route_concept_id,
  drug_source_value,
  dosage,
  adm_route,
  last_edit_time,
  updated_at
from lth_bronze.stg__drug_exposure as de
inner join (
  select distinct
    source_code,
    target_concept_id
  from lth_bronze.vocab__source_to_concept_map 
  where
    [group] = 'drugs'
) as c_cm
  on cast(de.flex_procedure_id as varchar) = c_cm.source_code
inner join (
  select
    target_concept_id,
    source_code
  from lth_bronze.vocab__source_to_concept_map 
  where
    [group] = 'drug_routes'
) as r_cm
  on cast(de.adm_route as varchar) = r_cm.source_code
