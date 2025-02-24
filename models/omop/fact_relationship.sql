
MODEL (
  name lth_bronze.fact_relationship,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

select
  domain_concept_id_1::bigint,
  fact_id_1::bigint,
  domain_concept_id_2::bigint,
  fact_id_2::bigint,
  relationship_concept_id::bigint,
  unique_key,
  last_edit_time::datetime
from lth_bronze.fr__observation_to_measurement_sl

union all

select
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  unique_key,
  last_edit_time::datetime
from lth_bronze.fr__measurement_to_observation_sl

union all

select
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  unique_key,
  last_edit_time::datetime
from lth_bronze.fr__specimen_to_measurement

union all

select
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  unique_key,
  last_edit_time::datetime
from lth_bronze.fr__mother_to_child