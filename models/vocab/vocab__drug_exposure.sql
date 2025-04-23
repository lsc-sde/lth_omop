
MODEL (
  name lth_bronze.vocab__drug_exposure,
  kind FULL,
  cron '@daily',
);

with drugs as (
select
  mpi.person_id,
  fdo.visit_occurrence_id,
  fdo.drug_exposure_start_datetime,
  fdo.provider_id,
  fdo.flex_procedure_id,
  fdo.drug_source_value,
  fdo.dosage,
  fdo.adm_route,
  fdo.org_code,
  fdo.source_system,
  fdo.last_edit_time,
  fdo.updated_at
from lth_bronze.stg_flex__drug_exposure as fdo
inner join lth_bronze.stg__master_patient_index as mpi
  on fdo.person_source_value = mpi.flex_patient_id
)

select distinct
  visit_occurrence_id,
  person_id,
  c_cm.target_concept_id,
  drug_exposure_start_datetime,
  provider_id,
  32838 as drug_type_concept_id,
  r_cm.target_concept_id as route_concept_id,
  drug_source_value,
  dosage,
  adm_route,
  org_code::varchar(5),
  source_system::varchar(20),
  last_edit_time,
  updated_at
from drugs as de
inner join (
  select distinct
    source_code,
    target_concept_id
  from lth_bronze.vocab__source_to_concept_map
  where
    concept_group = 'drugs'
) as c_cm
  on cast(de.flex_procedure_id as varchar) = c_cm.source_code
inner join (
  select
    target_concept_id,
    source_code
  from lth_bronze.vocab__source_to_concept_map
  where
    concept_group = 'drug_routes'
) as r_cm
  on cast(de.adm_route as varchar) = r_cm.source_code
  