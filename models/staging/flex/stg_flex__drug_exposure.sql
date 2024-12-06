{{
  config(
    materialized = "view",
    tags = ['bi', 'flex', 'staging', 'drugs']
    )
}}

with drug_exp as (
  select
    visit_id as visit_occurrence_id,
    person_source_value,
    cast(
      cast((event_id / 864000) - 21550 as datetime) as datetime2(0)
    ) as procedure_datetime,
    provider_source_value as provider_id,
    flex_procedure_id,
    flex_procedure_name as procedure_source_value,
    adm_route,
    last_edit_time,
    updated_at,
    replace(dosage, ' ' + adm_route, '') as dosage
  from lth_bronze.src_flex__procedure_event 
  where
    kardex_group_id in (17, 43, 44)
    and event_status_id in (6, 11)
),

visit_detail as (
  select distinct
    visit_number,
    visit_id,
    first_visit_id,
    person_source_value
  from lth_bronze.stg_flex__facility_transfer 
)

select
  c.person_source_value,
  cast(procedure_datetime as date) as drug_exposure_start_date,
  procedure_datetime as drug_exposure_start_datetime,
  cast(provider_id as varchar) as provider_id,
  flex_procedure_id,
  dosage,
  adm_route,
  last_edit_time,
  updated_at,
  replace(procedure_source_value, ' (CRITICAL MED)', '') as drug_source_value,
  isnull(v.first_visit_id, c.visit_occurrence_id) as visit_occurrence_id
from drug_exp as c
left join visit_detail as v
  on c.visit_occurrence_id = v.visit_id
