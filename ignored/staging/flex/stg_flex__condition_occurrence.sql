
MODEL (
  name lth_bronze.stg_flex__condition_occurrence,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (last_edit_time, '%Y-%m-%d %H:%M:%S.%f'),
    batch_size 30
  ),
  cron '@daily',
  grain (patient_id, visit_occurrence_id, source_code, data_source)
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
  isnull(v.person_source_value, vs.person_source_value) as patient_id,
  sfvd.episode_start_dt,
  sfvd.episode_end_dt,
  sfvd.provider_source_value as provider_id,
  replace(
      left(sfvd.icd10_code, 3) + '.' + substring(sfvd.icd10_code, 4, 10), '.X', ''
    ) as source_code,
  sfvd.index_nbr,
  sfvd.last_edit_time,
  sfvd.updated_at,
  isnull(v.first_visit_id, vs.visit_id) as visit_occurrence_id,
  'ukcoder' as data_source
from lth_bronze.src_flex__vtg_diagnosis  as sfvd
left join
  (
    select distinct
      visit_number,
      first_visit_id,
      person_source_value
    from
      visit_detail
  ) as v
  on sfvd.visit_number = v.visit_number
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
    sfvd.visit_number = vs.visit_number
    and v.first_visit_id is null
    and sfvd.last_edit_time::DATETIME between @start_dt and @end_dt

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
where ae.last_edit_time::DATETIME between @start_dt and @end_dt
