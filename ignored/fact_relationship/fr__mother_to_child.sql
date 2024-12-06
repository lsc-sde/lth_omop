
MODEL (
  name lth_bronze.fr__mother_to_child,
  kind FULL,
  cron '@daily',
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
