MODEL (
  name vcb.vocab__measurement,
  kind VIEW,
  cron '@daily'
);

SELECT DISTINCT
  r.person_id,
  r.visit_occurrence_id,
  NULL AS visit_detail_id,
  r.measurement_event_id::VARCHAR(80),
  r.provider_id,
  r.result_datetime,
  cm.target_concept_id,
  32817 AS type_concept_id,
  replace(replace(r.source_value, '<', ''), '>', '')::FLOAT AS value_as_number,
  coalesce(dc.target_concept_id, rs.concept_id) AS value_as_concept_id,
  um.target_concept_id AS unit_concept_id,
  NULL AS range_low,
  NULL AS range_high,
  r.source_name AS source_value,
  NULL AS measurement_source_concept_id,
  isnull(r.unit_source_value, um.source_code_description) AS unit_source_value,
  NULL AS unit_source_concept_id,
  r.value_source_value AS value_source_value,
  NULL AS meas_event_field_concept_id,
  CASE
    WHEN r.source_value LIKE '%>%'
    THEN 4172704
    WHEN r.source_value LIKE '%<%'
    THEN 4171756
  END AS operator_concept_id,
  r.org_code,
  r.source_system,
  r.updated_at,
  r.last_edit_time
FROM stg.stg__result AS r
INNER JOIN (
  SELECT DISTINCT
    source_code AS source_code,
    target_concept_id AS target_concept_id,
    target_domain_id AS target_domain_id,
    concept_group AS concept_group
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'result'
    OR (
      (
        concept_group = 'bacteria_presence'
        OR concept_group = 'bacteria_sensitivities'
        OR concept_group = 'bacteriology_other_test'
      )
      AND source_system = 'swisslab'
    )
) AS cm
  ON r.source_code = cm.source_code
LEFT JOIN (
  SELECT
    source_code AS source_code,
    source_code_description AS source_code_description,
    target_concept_id AS target_concept_id,
    target_domain_id AS target_domain_id
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'units'
) AS um
  ON r.source_code = um.source_code
LEFT JOIN (
  SELECT
    target_concept_id AS target_concept_id,
    source_code AS source_code,
    source_code_description AS source_code_description
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group IN ('decoded', 'bacteriology_other_result')
) AS dc
  ON r.source_code = dc.source_code
  AND r.value_source_value = dc.source_code_description
LEFT JOIN (
  SELECT
    concept_id AS concept_id,
    concept_name AS concept_name
  FROM @catalog_src.@schema_vocab.concept
  WHERE
    concept_name IN ('Positive', 'Negative', 'Sensitive', 'Indeterminate', 'Resistant')
    AND domain_id IN ('Meas Value')
    AND vocabulary_id IN ('SNOMED')
) AS rs
  ON r.value_source_value = rs.concept_name
WHERE
  cm.target_domain_id = 'Measurement'
