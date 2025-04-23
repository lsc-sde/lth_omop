
MODEL (
  name lth_bronze.vocab__visit_detail,
  kind FULL,
  cron '@daily',
);

with visit_detail as (
select
  vd.visit_detail_id::numeric as visit_detail_id,
  vd.visit_id,
  mpi.person_id,
  visit_type,
  location_id,
  vd.checkin_datetime,
  vd.checkout_datetime,
  vd.last_edit_time,
  vd.source_system,
  vd.org_code,
  updated_at
from lth_bronze.stg_flex__visit_detail as vd
inner join lth_bronze.stg__master_patient_index as mpi
  on mpi.flex_patient_id = vd.patient_id
)

select
  vd.visit_detail_id,
  vd.person_id,
  vd.checkin_datetime as visit_detail_start_datetime,
  vd.checkout_datetime as visit_detail_end_datetime,
  32817 as visit_detail_type_concept_id,
  null as provider_id,
  vd.visit_type as visit_detail_source_value,
  null as visit_detail_source_concept_id,
  null as admitted_from_concept_id,
  null as admitted_from_source_value,
  null as discharged_to_source_value,
  null as discharged_to_concept_id,
  null as parent_visit_detail_id,
  vd.visit_id as visit_occurrence_id,
  case
    when vd.visit_type = 'ER' then 9203
    when vd.visit_type = 'ERIP' then 262
    when vd.visit_type = 'IP' then 9201
  end as visit_detail_concept_id,
  left(
    vd.location_id, charindex('-', vd.location_id + '-') - 1
  ) as location_id,
  lag(vd.visit_detail_id) over (
    partition by vd.person_id order by vd.checkin_datetime
  ) as preceding_visit_detail_id,  
  source_system::varchar(20),
  org_code::varchar(5)
from visit_detail as vd
