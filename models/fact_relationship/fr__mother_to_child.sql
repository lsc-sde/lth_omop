{{
  config(
    materialized = "incremental",
    unique_key= ['unique_key']
    )
}}

select
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  unique_key,
  last_edit_time
from {{ ref('cdc_fr__mother_to_child') }}
