MODEL (
  name lth_bronze.vocab__source_to_concept_map_raw,
  kind VIEW,
  cron '@daily'
);

SELECT
  source_code,
  source_code_description,
  target_concept_id,
  target_concept_name,
  target_domain_id,
  concept_group,
  source_system::VARCHAR(50) AS source_system,
  frequency::INTEGER AS frequency,
  mapping_status::VARCHAR(50) AS mapping_status
FROM (
  @generate_source_to_concept_map()
) AS cm
