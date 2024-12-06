
MODEL (
  name lth_bronze.stg__visit_detail,
  kind FULL,
  cron '@daily',
);

select
  cast(vd.visit_detail_id as numeric) as visit_detail_id,
  vd.visit_id,
  mpi.person_id,
  visit_type,
  location_id,
  vd.checkin_datetime,
  vd.checkout_datetime,
  vd.last_edit_time,
  updated_at
from lth_bronze.stg_flex__visit_detail as vd
inner join lth_bronze.stg__master_patient_index as mpi
  on mpi.flex_patient_id = vd.patient_id
