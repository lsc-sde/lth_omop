MODEL (
  name vcb.vocab__condition_occurrence,
  kind FULL,
  cron '@daily'
);

WITH concept AS (
  SELECT DISTINCT
    concept_id AS concept_id,
    replace(concept_code, '.', '') AS concept_code,
    invalid_reason AS invalid_reason
  FROM @catalog_src.@schema_vocab.concept
  WHERE
    vocabulary_id IN ('ICD10CM', 'SNOMED')
), mappings AS (
  SELECT
    source.concept_code AS icd_code,
    source.concept_name AS icd_name,
    target.concept_id AS snomed_code,
    target.concept_name AS snomed_name
  FROM @catalog_src.@schema_vocab.concept AS source
  INNER JOIN @catalog_src.@schema_vocab.concept_relationship AS rel
    ON source.concept_id = rel.concept_id_1
    AND rel.invalid_reason IS NULL
    AND rel.relationship_id = 'Maps to'
  INNER JOIN @catalog_src.@schema_vocab.concept AS target
    ON rel.concept_id_2 = target.concept_id AND target.invalid_reason IS NULL
  WHERE
    source.vocabulary_id = 'ICD10CM' AND target.vocabulary_id = 'SNOMED'
), snomed_mappings AS (
  SELECT
    source.concept_id AS snomed_code,
    source.concept_code AS concept_snomed,
    source.concept_name AS snomed_name,
    target.concept_id AS new_snomed_code,
    target.concept_name AS new_snomed_name
  FROM @catalog_src.@schema_vocab.concept AS source
  INNER JOIN @catalog_src.@schema_vocab.concept_relationship AS rel
    ON source.concept_id = rel.concept_id_1
    AND rel.invalid_reason IS NULL
    AND rel.relationship_id = 'Maps to'
  INNER JOIN @catalog_src.@schema_vocab.concept AS target
    ON rel.concept_id_2 = target.concept_id AND target.invalid_reason IS NULL
), condition AS (
  SELECT
    person_id AS person_id,
    isnull(mp.snomed_code, isnull(sp.new_snomed_code, cm.concept_id)) AS condition_concept_id,
    episode_start_dt AS condition_start_date,
    episode_end_dt AS condition_end_date,
    CASE WHEN source_system = 'scr' THEN 32879 ELSE 32817 END AS condition_type_concept_id,
    CASE
      WHEN source_system = 'scr'
      THEN 32902
      WHEN episode_coding_position = 1
      THEN 32903
      ELSE 32909
    END AS condition_status_concept_id,
    provider_id AS provider_id,
    isnumeric(provider_id) AS provider_id_type,
    visit_occurrence_id AS visit_occurrence_id,
    co.source_code AS condition_source_value,
    cm.concept_id AS condition_source_concept_id,
    org_code AS org_code,
    source_system AS source_system,
    last_edit_time AS last_edit_time,
    updated_at AS updated_at
  FROM stg.stg__condition_occurrence AS co
  INNER JOIN concept AS cm
    ON replace(co.source_code, '.', '') = cm.concept_code
  LEFT JOIN mappings AS mp
    ON replace(co.source_code, '.', '') = replace(mp.icd_code, '.', '')
    AND co.source_system IN ('ukcoder')
  LEFT JOIN snomed_mappings AS sp
    ON co.source_code = sp.concept_snomed AND co.source_system = 'flex_ae'
)
SELECT DISTINCT
  c.*
FROM condition AS c
INNER JOIN @catalog_src.@schema_vocab.concept AS cn
  ON c.condition_concept_id = cn.concept_id
WHERE
  cn.domain_id = 'Condition'
