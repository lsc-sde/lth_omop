MODEL (
  name ext.ext__data_opt_out,
  kind FULL,
  cron '@daily'
);

SELECT
  mrn AS mrn,
  nhsnumber AS nhs_number,
  mpi.person_id,
  mpi.person_source_value::BIGINT AS person_source_value,
  insertdatetime AS insert_datetime
FROM @catalog_src.admin.data_opt_out AS doo
LEFT JOIN stg.stg__master_patient_index AS mpi
  ON doo.mrn = mpi.flex_mrn
