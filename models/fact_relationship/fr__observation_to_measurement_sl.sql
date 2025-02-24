
MODEL (
  name lth_bronze.fr__observation_to_measurement_sl,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

select
  ob.domain_concept_id_1,
  ob.fact_id_1,
  ob.domain_concept_id_2,
  ob.fact_id_2,
  ob.relationship_concept_id,
  ob.observation_key,
  ob.measurement_key, 
  ob.unique_key,
  ob.last_edit_time
from lth_bronze.cdc_fr__observation_to_measurement as ob