
MODEL (
  name lth_bronze.source_to_concept_map,
  kind FULL,
  cron '@daily',
);

select
  source_code::VARCHAR(50) as source_code,
  0::INT as source_concept_id,
  44819096::VARCHAR(8) as source_vocabulary_id,
  source_code_description::VARCHAR(255) as source_code_description,
  target_concept_id::BIGINT as target_concept_id,
  c.vocabulary_id::VARCHAR(20) as target_vocabulary_id,
  c.valid_start_date::DATE,
  c.valid_end_date::DATE,
  c.invalid_reason::VARCHAR,
  frequency::INT,
  'rxn'::varchar(5) as org_code
from lth_bronze.vocab__source_to_concept_map_raw as r
left join @catalog_src.@schema_vocab.CONCEPT as c
  on r.target_concept_id = c.concept_id
where
  mapping_status = 'approved'
  and source_code is not null
