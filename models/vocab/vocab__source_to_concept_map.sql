{{
  config(
    materialized = "table",
    tags = ['mappings', 'vocab'],
    as_columnstore = false,
    post_hook = [
      "CREATE INDEX idx_vstcm_source_code ON {{ this }} (source_code ASC);",
      "CREATE INDEX idx_vstcm_source_code_description ON {{ this }} (source_code_description ASC);",
      "CREATE INDEX idx_vstcm_source_code_group ON {{ this }} ([group] ASC);",
    ]
    )
}}

select distinct
  source_code,
  source_code_description,
  target_concept_id,
  target_concept_name,
  target_domain_id,
  --frequency,
  "group",
  source,
  mapping_status
from
  lth_bronze.vocab__source_to_concept_map_raw 
where mapping_status = 'APPROVED'
