
MODEL (
  name lth_bronze.stg_flex__drug_exposure,
  kind VIEW,
  cron '@daily'
);

with visit_detail as (
  select distinct
    visit_number,
    visit_id,
    first_visit_id,
    person_source_value
  from lth_bronze.stg_flex__facility_transfer 
)

select
  c.person_source_value,
  @convert_event_ts_to_datetime(event_ts) as drug_exposure_start_datetime,
  cast(drug_exposure_start_datetime as date) as drug_exposure_date,
  cast(provider_id as varchar) as provider_id,
  flex_procedure_id,
  dosage,
  adm_route,
  source_system::varchar(20),
  org_code::varchar(5),
  last_edit_time,
  updated_at,
  replace(procedure_source_value, ' (CRITICAL MED)', '') as drug_source_value,
  isnull(v.first_visit_id, c.visit_occurrence_id) as visit_occurrence_id
from lth_bronze.cdc_flex__drug_exposure as c
left join visit_detail as v
  on c.visit_occurrence_id = v.visit_id
