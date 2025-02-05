
MODEL (
  name lth_bronze.vocab__visit_occurrence,
  kind FULL,
  cron '@daily',
);

with visits as (
    select
        mpi.person_id,
        fvo.visit_number,
        fvo.activation_time,
        fvo.visit_type_id,
        fvo.visit_status_id,
        fvo.visit_id,
        fvo.admission_time,
        fvo.discharge_time,
        fvo.provider_id,
        fvo.facility_id,
        fvo.admitted_from_source_value,
        fvo.discharged_to_source_value,
        fvo.last_edit_time,
        fvo.updated_at
    from lth_bronze.stg_flex__visit_occurrence as fvo
    inner join lth_bronze.stg__master_patient_index as mpi
    on fvo.person_source_value = mpi.flex_patient_id
    where fvo.visit_type_id is not null
),

ae_visits as (
  select distinct visit_id from lth_bronze.src_flex__visit_detail_ae
)

select
  vo.visit_id as visit_occurrence_id,
  vo.person_id,
  case
    when vo.visit_type_id in (3, 4) then isnull(discharge_time, admission_time)
    when vo.visit_status_id in (7) then isnull(discharge_time, admission_time)
    else discharge_time
  end as visit_end_datetime,
  32817 as visit_type_concept_id,
  provider_id,
  facility_id as care_site_id,
  null as visit_source_concept_id,
  vo.admitted_from_source_value,
  vo.discharged_to_source_value,
  case
    when vo.visit_type_id in (1, 6) and a.visit_id is not null then 262
    when vo.visit_type_id in (1, 6) then 9201
    when vo.visit_type_id = 2 then 9203
    when vo.visit_type_id in (3, 4) then 9202
    when vo.visit_type_id in (5, 8, 7, 9) then 32761
  end as visit_concept_id,
  case
    when
      vo.visit_type_id in (1, 6) and a.visit_id is not null
      then activation_time
    else admission_time
  end as visit_start_datetime,
  case
    when vo.visit_type_id in (1, 6) and a.visit_id is not null then 'ERIP'
    when vo.visit_type_id in (1, 6) then 'IP'
    when vo.visit_type_id = 2 then 'ER'
    when vo.visit_type_id in (3, 4) then 'OP'
    when vo.visit_type_id in (5, 8, 7, 9) then 'PUI'
    else 'ER'
  end as visit_source_value,
  case
    when csa.target_concept_id = '4139502' then 0 else csa.target_concept_id
  end as admitted_from_concept_id,
  case
    when csd.target_concept_id = '4139502' then null else csd.target_concept_id
  end as discharged_to_concept_id,
  vo.visit_status_id,
  vo.visit_type_id,
  vo.last_edit_time,
  vo.updated_at
from visits as vo
left join
  (
    select
      target_concept_id,
      source_code
    from lth_bronze.vocab__source_to_concept_map
    where concept_group = 'discharge_destination'
  ) as csd
  on vo.discharged_to_source_value = csd.source_code
left join
  (
    select
      target_concept_id,
      source_code
    from lth_bronze.vocab__source_to_concept_map
    where concept_group = 'admission_source'
  ) as csa
  on vo.admitted_from_source_value = csa.source_code
left join ae_visits as a
  on vo.visit_id = a.visit_id

