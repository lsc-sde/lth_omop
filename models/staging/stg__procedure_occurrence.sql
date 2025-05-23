
MODEL (
  name lth_bronze.stg__procedure_occurrence,
  kind FULL,
  cron '@daily',
);

with person as (
select
    *,
    row_number() over (partition by person_id order by last_edit_time desc) as id
from lth_bronze.stg__master_patient_index
)

select distinct
  mpi.person_id,
  fpo.visit_occurrence_id,
  fpo.procedure_date,
  fpo.procedure_datetime,
  fpo.provider_id,
  isnumeric(fpo.provider_id) as provider_id_type,
  fpo.procedure_source_value,
  fpo.source_code,
  fpo.org_code,
  fpo.source_system,
  fpo.last_edit_time,
  fpo.updated_at
from lth_bronze.stg_flex__procedure_occurrence as fpo
inner join person as mpi
  on fpo.person_source_value = mpi.flex_patient_id
  and id = 1