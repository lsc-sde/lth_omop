
MODEL (
  name lth_bronze.stg__visit_occurrence,
  kind FULL,
  cron '@daily',
);

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
