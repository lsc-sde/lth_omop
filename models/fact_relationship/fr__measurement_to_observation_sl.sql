{{
  config(
    materialized = "incremental",
    unique_key= ['measurement_key', 'observation_key']
    )
}}

select distinct
  slb.domain_concept_id_1,
  slb.fact_id_1,
  slb.domain_concept_id_2,
  slb.fact_id_2,
  slb.relationship_concept_id,
  slb.observation_key,
  slb.measurement_key,
  slb.unique_key,
  slb.last_edit_time
from {{ ref('cdc_fr__measurement_to_observation_sl') }} as slb
