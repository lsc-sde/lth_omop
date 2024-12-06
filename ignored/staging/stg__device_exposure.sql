
MODEL (
  name lth_bronze.stg__device_exposure,
  kind FULL,
  cron '@daily',
);

select
  dvc.visit_id,
  vd.visit_detail_id,
  mpi.person_id,
  dvc.date_time as device_datetime,
  manufacturer as device_manufacturer,
  device_type,
  device_type_group,
  device_lot_number,
  device_details,
  expiry_date
from lth_bronze.stg_flex__devices as dvc
left join lth_bronze.stg__visit_detail as vd
  on
    dvc.visit_id = vd.visit_id
    and dvc.date_time >= vd.checkin_datetime
    and dvc.date_time < vd.checkout_datetime
inner join lth_bronze.stg__master_patient_index as mpi
  on dvc.patient_id = mpi.flex_patient_id
