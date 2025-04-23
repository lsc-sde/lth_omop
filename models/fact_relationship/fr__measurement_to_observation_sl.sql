
MODEL (
  name lth_bronze.fr__measurement_to_observation_sl,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

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
from lth_bronze.cdc_fr__measurement_to_observation_sl as slb