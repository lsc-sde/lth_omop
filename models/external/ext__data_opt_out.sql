{{
  config(
    materialized = "table",
    tags = ['lookup', 'dimension', 'ext']
    )
}}

select
  MRN as mrn,
  NHSNumber as nhs_number,
  mpi.person_id,
  cast(mpi.person_source_value as bigint) as person_source_value,
  InsertDateTime as insert_datetime
from admin.Data_Opt_Out doo
left join {{ ref('stg__master_patient_index') }} mpi
on doo.MRN = mpi.flex_mrn
