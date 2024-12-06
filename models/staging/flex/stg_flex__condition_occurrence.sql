{{
  config(
    materialized = "table",
    tags = ['bi', 'flex', 'staging', 'condition']
    )
}}

with condition_occ as (
  select
    visit_number,
    episode_start_dt,
    episode_end_dt,
    index_nbr,
    provider_source_value,
    last_edit_time,
    updated_at,
    replace(
      left(icd10_code, 3) + '.' + substring(icd10_code, 4, 10), '.X', ''
    ) as source_code
  from lth_bronze.src_flex__vtg_diagnosis 
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
  isnull(v.person_source_value, vs.person_source_value) as patient_id,
  c.episode_start_dt,
  c.episode_end_dt,
  c.provider_source_value as provider_id,
  c.source_code,
  c.index_nbr,
  c.last_edit_time,
  c.updated_at,
  isnull(v.first_visit_id, vs.visit_id) as visit_occurrence_id,
  'ukcoder' as data_source
from condition_occ as c
left join
  (
    select distinct
      visit_number,
      first_visit_id,
      person_source_value
    from
      visit_detail
  ) as v
  on c.visit_number = v.visit_number
left join
  (
    select distinct
      visit_number,
      visit_id,
      person_source_value
    from lth_bronze.src_flex__visit_segment 
    where
      visit_number not in (select distinct visit_number from visit_detail)
  ) as vs
  on
    c.visit_number = vs.visit_number
    and v.first_visit_id is null

union

select
  ae.patient_id,
  activation_time,
  case
    when
      activation_time = discharge_date_time then discharge_date_time
    else admission_date_time
  end as condition_end_date,
  cast(provider_id as varchar) as provider_id,
  diagnosis_list,
  null,
  last_edit_time,
  updated_at,
  coalesce(v.first_visit_id, ae.visit_id) as visit_occurrence_id,
  'ae_diagnosis' as data_source
from lth_bronze.stg_flex__ae_diagnosis as ae
left join
  (
    select distinct
      visit_id,
      first_visit_id,
      person_source_value
    from
      visit_detail
  ) as v
  on ae.visit_id = v.visit_id
