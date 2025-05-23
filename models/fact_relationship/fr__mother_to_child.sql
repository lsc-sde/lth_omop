MODEL (
  name lth_bronze.fr__mother_to_child,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

select
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  unique_key,
  last_edit_time
from lth_bronze.cdc_fr__mother_to_child