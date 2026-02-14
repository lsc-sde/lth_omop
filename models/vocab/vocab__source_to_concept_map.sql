MODEL (
  name lth_bronze.vocab__source_to_concept_map,
  kind FULL,
  cron '@daily'
);

SELECT DISTINCT
  source_code::VARCHAR(MAX) AS source_code,
  source_code_description::VARCHAR(MAX) AS source_code_description,
  target_concept_id::INTEGER,
  target_concept_name::VARCHAR(MAX) AS target_concept_name,
  target_domain_id::VARCHAR(50) AS target_domain_id,
  concept_group::VARCHAR(50) AS concept_group,
  source_system::VARCHAR(50) AS source_system,
  mapping_status::VARCHAR(50) AS mapping_status
FROM lth_bronze.vocab__source_to_concept_map_raw
WHERE
  mapping_status = 'APPROVED'
