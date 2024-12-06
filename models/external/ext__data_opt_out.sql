
MODEL (
  name lth_bronze.ext__data_opt_out,
  kind FULL,
  cron '@daily',
);

select
  MRN as mrn,
  NHSNumber as nhs_number,
  mpi.person_id,
  cast(mpi.person_source_value as bigint) as person_source_value,
  InsertDateTime as insert_datetime
from admin.Data_Opt_Out doo
left join lth_bronze.stg__master_patient_index mpi
on doo.MRN = mpi.flex_mrn
