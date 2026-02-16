MODEL (
  name vcb.vocab__procedure_occurrence,
  kind FULL,
  cron '@daily'
);

WITH concept AS (
  SELECT
    concept_id,
    concept_code,
    concept_name
  FROM dbt_omop.vocab.concept
  WHERE
    vocabulary_id IN ('OPCS4', 'SNOMED')
  UNION
  SELECT
    target_concept_id,
    source_code,
    target_concept_name
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'radiology'
), procs AS (
  SELECT
    po.person_id AS person_id,
    po.visit_occurrence_id AS visit_occurrence_id,
    po.procedure_date AS procedure_date,
    po.procedure_datetime AS procedure_datetime,
    po.provider_id AS provider_id,
    provider_id_type AS provider_id_type,
    isnull(po.procedure_source_value, cm.concept_name) AS procedure_source_value,
    32817 AS procedure_type_concept_id,
    po.source_code AS source_code,
    cm.concept_id AS concept_id,
    last_edit_time AS last_edit_time,
    org_code AS org_code,
    source_system AS source_system
  FROM stg.stg__procedure_occurrence AS po
  INNER JOIN concept AS cm
    ON po.source_code = cm.concept_code
), prov AS (
  SELECT
    cons_org_code AS cons_org_code,
    provider_id AS provider_id
  FROM vcb.vocab__provider
  WHERE
    NOT cons_org_code IS NULL
), prov1 AS (
  SELECT
    provider_id AS provider_id,
    provider_source_value AS provider_source_value
  FROM vcb.vocab__provider
)
SELECT DISTINCT
  p.person_id,
  p.visit_occurrence_id,
  p.procedure_date,
  p.procedure_datetime,
  coalesce(pr.provider_id, pr1.provider_id) AS provider_id,
  p.procedure_source_value,
  p.procedure_type_concept_id,
  p.source_code,
  p.concept_id,
  last_edit_time,
  p.org_code,
  p.source_system
FROM procs AS p
INNER JOIN dbt_omop.vocab.concept AS cn
  ON p.concept_id = cn.concept_id
LEFT JOIN prov AS pr
  ON pr.cons_org_code = p.provider_id AND provider_id_type = 0
LEFT JOIN prov1 AS pr1
  ON pr1.provider_source_value = p.provider_id AND provider_id_type = 1
WHERE
  cn.domain_id = 'Procedure' AND cn.invalid_reason IS NULL
