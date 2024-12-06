{{
  config(
    materialized = "view",
    tags = ['drugs', 'staging'],
    docs = {
        'name': 'stg__drug_exposure',
        'description': 'Drug exposures view for all sources'
    }
    )
}}


select
  mpi.person_id,
  fdo.visit_occurrence_id,
  fdo.drug_exposure_start_date,
  fdo.drug_exposure_start_datetime,
  fdo.provider_id,
  fdo.flex_procedure_id,
  fdo.drug_source_value,
  fdo.dosage,
  fdo.adm_route,
  fdo.last_edit_time,
  fdo.updated_at
from {{ ref('stg_flex__drug_exposure') }} as fdo
inner join {{ ref('stg__master_patient_index') }} as mpi
  on fdo.person_source_value = mpi.flex_patient_id
