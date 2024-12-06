{{
  config(
    materialized = "incremental",
    unique_key= ['measurement_key']
    )
}}

select
  s.domain_concept_id_1,
  s.fact_id_1,
  s.domain_concept_id_2,
  s.fact_id_2,
  s.relationship_concept_id,
  s.specimen_event_id,
  s.measurement_key,
  s.unique_key,
  s.last_edit_time
from {{ ref('cdc_fr__specimen_to_measurement') }} as s
