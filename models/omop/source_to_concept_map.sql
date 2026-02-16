MODEL (
  name cdm.source_to_concept_map,
  kind FULL,
  cron '@daily'
);

SELECT
  source_code::VARCHAR(50) AS source_code,
  0::INTEGER AS source_concept_id,
  44819096::VARCHAR(8) AS source_vocabulary_id,
  source_code_description::VARCHAR(255) AS source_code_description,
  target_concept_id::BIGINT AS target_concept_id,
  c.vocabulary_id::VARCHAR(20) AS target_vocabulary_id,
  c.valid_start_date::DATE,
  c.valid_end_date::DATE,
  c.invalid_reason::VARCHAR,
  frequency::INTEGER,
  'rxn'::VARCHAR(5) AS org_code
FROM vcb.vocab__source_to_concept_map_raw AS r
LEFT JOIN @catalog_src.@schema_vocab.concept AS c
  ON r.target_concept_id = c.concept_id
WHERE
  mapping_status = 'approved' AND NOT source_code IS NULL
