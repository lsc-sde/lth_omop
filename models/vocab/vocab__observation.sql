MODEL (
  name lth_bronze.vocab__observation,
  kind VIEW,
  cron '@daily'
);

WITH concept AS (
  SELECT DISTINCT
    concept_id AS concept_id,
    concept_code AS concept_code
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
), condition AS (
  SELECT
    person_id AS person_id,
    isnull(mp.snomed_code, cm.concept_id) AS condition_concept_id,
    episode_start_dt AS condition_start_date,
    episode_end_dt AS condition_end_date,
    32817 AS condition_type_concept_id,
    CASE WHEN episode_coding_position = 1 THEN 32903 ELSE 32909 END AS condition_status_concept_id,
    provider_id AS provider_id,
    visit_occurrence_id AS visit_occurrence_id,
    co.source_code AS condition_source_value,
    cm.concept_id AS condition_source_concept_id,
    co.org_code AS org_code,
    co.source_system AS source_system,
    co.last_edit_time AS last_edit_time,
    co.updated_at AS updated_at
  FROM lth_bronze.stg__condition_occurrence AS co
  INNER JOIN concept AS cm
    ON co.source_code = cm.concept_code
  LEFT JOIN mappings AS mp
    ON co.source_code = mp.icd_code
)
SELECT DISTINCT
  r.person_id,
  r.visit_occurrence_id,
  NULL AS visit_detail_id,
  r.measurement_event_id AS observation_event_id,
  r.provider_id::VARCHAR AS provider_id,
  r.result_datetime,
  cm.target_concept_id,
  32817 AS type_concept_id,
  r.source_value AS value_as_number,
  r.value_source_value AS value_as_string,
  coalesce(cm_dc.target_concept_id, cm_rp.target_concept_id) AS value_as_concept_id,
  NULL AS qualifier_concept_id,
  NULL AS unit_concept_id,
  r.source_name AS source_value,
  NULL AS observation_source_concept_id,
  NULL AS unit_source_value,
  NULL::VARCHAR(50) AS qualifier_source_value,
  r.value_source_value,
  NULL AS obs_event_field_concept_id,
  r.org_code,
  r.source_system,
  r.updated_at AS last_edit_time
FROM lth_bronze.stg__result AS r
INNER JOIN (
  SELECT
    source_code AS source_code,
    target_concept_id AS target_concept_id,
    target_domain_id AS target_domain_id
  FROM lth_bronze.vocab__source_to_concept_map
  WHERE
    concept_group = 'result'
    OR (
      concept_group = 'bacteria_observation' AND source_system = 'swisslab'
    )
) AS cm
  ON r.source_code = cm.source_code
LEFT JOIN (
  SELECT DISTINCT
    source_code AS source_code,
    target_concept_id AS target_concept_id,
    target_domain_id AS target_domain_id
  FROM lth_bronze.vocab__source_to_concept_map
  WHERE
    concept_group IN ('referral_priority')
) AS cm_rp
  ON r.priority = cm_rp.source_code
LEFT JOIN (
  SELECT DISTINCT
    source_code AS source_code,
    source_code_description AS source_code_description,
    target_concept_id AS target_concept_id,
    target_domain_id AS target_domain_id
  FROM lth_bronze.vocab__source_to_concept_map
  WHERE
    concept_group IN ('decoded')
) AS cm_dc
  ON r.source_code = cm_dc.source_code
  AND r.value_source_value = cm_dc.source_code_description
WHERE
  cm.target_domain_id = 'Observation'
UNION
SELECT DISTINCT
  c.person_id,
  c.visit_occurrence_id,
  NULL AS visit_detail_id,
  NULL::VARCHAR AS observation_event_id,
  provider_id,
  condition_start_date,
  condition_concept_id,
  32817 AS type_concept_id,
  NULL AS value_as_number,
  cn.concept_name AS value_as_string,
  NULL AS value_as_concept_id,
  NULL AS qualifier_concept_id,
  NULL AS unit_concept_id,
  c.condition_source_value AS source_value,
  c.condition_source_concept_id AS observation_source_concept_id,
  NULL AS unit_source_value,
  NULL::VARCHAR(50) AS qualifier_source_value,
  c.condition_source_value AS value_source_value,
  NULL AS obs_event_field_concept_id,
  org_code,
  source_system,
  c.updated_at
FROM condition AS c
INNER JOIN @catalog_src.@schema_vocab.concept AS cn
  ON c.condition_concept_id = cn.concept_id
WHERE
  cn.domain_id = 'Observation'
UNION
SELECT DISTINCT
  c.person_id,
  c.visit_occurrence_id,
  NULL AS visit_detail_id,
  NULL::VARCHAR AS observation_event_id,
  provider_id,
  condition_start_date,
  condition_concept_id,
  32817 AS type_concept_id,
  NULL AS value_as_number,
  cn.concept_name AS value_as_string,
  NULL AS value_as_concept_id,
  NULL AS qualifier_concept_id,
  NULL AS unit_concept_id,
  c.condition_source_value AS source_value,
  c.condition_source_concept_id AS observation_source_concept_id,
  NULL AS unit_source_value,
  NULL::VARCHAR(50) AS qualifier_source_value,
  c.condition_source_value AS value_source_value,
  NULL AS obs_event_field_concept_id,
  org_code,
  source_system,
  c.updated_at
FROM condition AS c
INNER JOIN @catalog_src.@schema_vocab.concept AS cn
  ON c.condition_concept_id = cn.concept_id
WHERE
  cn.domain_id = 'Observation'
UNION
SELECT DISTINCT
  person_id,
  NULL AS visit_occurrence_id,
  NULL AS visit_detail_id,
  NULL::VARCHAR AS observation_event_id,
  NULL AS provider_id,
  insert_datetime,
  44787910 AS concept_id,
  32848 AS type_concept_id,
  NULL AS value_as_number,
  'Informed dissent for national audit' AS value_as_string,
  NULL AS value_as_concept_id,
  NULL AS qualifier_concept_id,
  NULL AS unit_concept_id,
  'Informed dissent for national audit' AS source_value,
  NULL AS observation_source_concept_id,
  NULL AS unit_source_value,
  NULL::VARCHAR(50) AS qualifier_source_value,
  'Informed dissent for national audit' AS value_source_value,
  NULL AS obs_event_field_concept_id,
  'rxn' AS org_code,
  'mesh' AS source_system,
  insert_datetime AS updated_at
FROM lth_bronze.ext__data_opt_out
WHERE
  NOT person_id IS NULL
