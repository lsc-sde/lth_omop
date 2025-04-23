
MODEL (
  name lth_bronze.vocab__source_to_concept_map_raw,
  kind VIEW,
  cron '@daily',
);

select
  sourceCode::VARCHAR(MAX) as source_code,
  sourceName::VARCHAR(MAX) as source_code_description,
  conceptId::INT as target_concept_id,
  conceptName::VARCHAR(MAX) as target_concept_name,
  domainId::VARCHAR(50) as target_domain_id,
  concept_group::VARCHAR(50) as concept_group,
  source_system::VARCHAR(50) as source_system,
  sourceFrequency::INT as frequency,
  mappingStatus::VARCHAR(50) as mapping_status
from (
  @generate_source_to_concept_map()
) as cm
