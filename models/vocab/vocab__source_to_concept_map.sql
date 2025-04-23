
MODEL (
  name lth_bronze.vocab__source_to_concept_map,
  kind FULL,
  cron '@daily',
);

select distinct
  source_code,
  source_code_description,
  target_concept_id,
  target_concept_name,
  target_domain_id,
  --frequency,
  concept_group,
  source_system,
  mapping_status
from
  lth_bronze.vocab__source_to_concept_map_raw
where mapping_status = 'APPROVED'
