
MODEL (
  name lth_bronze.stg__condition_occurrence,
  kind FULL,
  cron '@daily',
);

with cond as (
  select
    mpi.person_id,
    fco.visit_occurrence_id,
    fco.episode_start_dt,
    fco.episode_end_dt,
    fco.provider_id,
    fco.source_code,
    fco.index_nbr as episode_coding_position,
	fco.org_code,
	fco.source_system,
    fco.last_edit_time,
    fco.updated_at
  from lth_bronze.stg_flex__condition_occurrence as fco
  inner join lth_bronze.stg__master_patient_index as mpi
    on fco.patient_id = mpi.flex_patient_id
),

person as (
  select
    *,
    row_number() over (partition by person_id order by last_edit_time desc)
      as id
  from lth_bronze.stg__master_patient_index
)

select
  person_id,
  visit_occurrence_id,
  episode_start_dt,
  episode_end_dt,
  provider_id,
  source_code,
  row_number() over (
    partition by visit_occurrence_id
    order by
      cast(episode_end_dt as date) desc,
      cast(episode_start_dt as date) desc,
      isnull(episode_coding_position, 99) asc
  ) as episode_coding_position,
  org_code,
  source_system,
  last_edit_time,
  updated_at
from cond

union

select
  coalesce(p1.person_id, p2.person_id) as person_id,
  null as visit_occurrence_id,
  date_of_diagnosis,
  null as end_date,
  null as provider_id,
  icd_code as source_code,
  1 as position,
  stg_scr.org_code,
  stg_scr.source_system,
  stg_scr.last_edit_time,
  stg_scr.updated_at
from lth_bronze.stg_scr__condition_occurrence as stg_scr
left join person as p1
  on
    cast(stg_scr.mrn as varchar) = cast(p1.flex_mrn as varchar)
    and p1.id = 1
left join person as p2
  on
    cast(stg_scr.nhs_number as varchar) = cast(p2.nhs_number as varchar)
    and p2.id = 1
where coalesce(p1.person_id, p2.person_id) is not null