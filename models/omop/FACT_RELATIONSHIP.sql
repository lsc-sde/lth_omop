{{
  config(
    materialized = "table",
    tags = ['omop', 'fact_relationship']
    )
}}

select
  cast(domain_concept_id_1 as bigint) as domain_concept_id_1,
  cast(fact_id_1 as bigint) as fact_id_1,
  cast(domain_concept_id_2 as bigint) as domain_concept_id_2,
  cast(fact_id_2 as bigint) as fact_id_2,
  cast(relationship_concept_id as bigint) as relationship_concept_id,
  cast(last_edit_time as datetime) as last_edit_time
from lth_bronze.fr__observation_to_measurement_sl 

union all

select
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  cast(last_edit_time as datetime) as last_edit_time
from lth_bronze.fr__measurement_to_observation_sl 

union all

select
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  cast(last_edit_time as datetime) as last_edit_time
from lth_bronze.fr__specimen_to_measurement 

union all

select
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  cast(last_edit_time as datetime) as last_edit_time
from lth_bronze.fr__mother_to_child 
