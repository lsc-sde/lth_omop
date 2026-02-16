MODEL (
  name stg.stg_sl__bacteriology,
  kind FULL,
  cron '@daily'
);

WITH merged AS (
  SELECT
    order_number,
    nhs_number,
    mrn,
    date_of_birth,
    sex,
    postcode,
    isolate_number,
    visit_number,
    location_code,
    clinician,
    clinician_code,
    order_date,
    result_date,
    site,
    qualifier,
    test,
    result_value,
    'live' AS source,
    updated_at,
    last_edit_time
  FROM src.src_sl__bacteriology_live
  WHERE
    NOT result_value IN (
      SELECT
        invalid_results
      FROM lth_bronze.swisslab__invalid_results
    )
    AND NOT result_value LIKE '#%'
  UNION ALL
  SELECT
    order_number,
    nhs_number,
    mrn,
    date_of_birth,
    sex,
    postcode,
    isolate_number,
    visit_number,
    location_code,
    clinician,
    clinician_code,
    order_date,
    result_date,
    site,
    qualifier,
    test,
    result_value,
    'archive' AS source,
    updated_at,
    last_edit_time
  FROM src.src_sl__bacteriology_archive
  WHERE
    NOT result_value IN (
      SELECT
        invalid_results
      FROM lth_bronze.swisslab__invalid_results
    )
    AND NOT result_value LIKE '#%'
), de_duped AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY order_number, isolate_number, test
      ORDER BY result_date DESC, source DESC
    ) AS sequence_id
  FROM merged
)
SELECT
  dd.nhs_number,
  mrn,
  date_of_birth,
  vo.visit_id AS visit_occurrence_id,
  @GENERATE_SURROGATE_KEY(nhs_number, order_number, isolate_number)::VARBINARY(16) AS isolate_event_id,
  @GENERATE_SURROGATE_KEY(nhs_number, order_number, isolate_number, test)::VARBINARY(16) AS measurement_event_id,
  order_date,
  pr.provider_id,
  site,
  qualifier,
  order_number,
  CASE WHEN test IN ('Organism ID', 'Neg. culture') THEN result_value ELSE test END AS source_name,
  CASE WHEN test IN ('Organism ID', 'Neg. culture') THEN result_value ELSE test END AS source_code,
  CASE
    WHEN test = 'Neg. culture'
    THEN 'Negative'
    WHEN test = 'Organism ID'
    THEN 'Positive'
    WHEN result_value = 'I'
    THEN 'Indeterminate'
    WHEN result_value = 'S'
    THEN 'Sensitive'
    WHEN result_value = 'R'
    THEN 'Resistant'
    WHEN result_value = '+'
    THEN 'Positive'
    WHEN result_value = '-'
    THEN 'Negative'
    ELSE result_value
  END AS value_source_value,
  NULL AS source_value,
  NULL AS value_as_number,
  NULL AS unit_source_value,
  NULL AS priority,
  'rxn' AS org_code,
  'swl' AS source_system,
  dd.updated_at,
  dd.last_edit_time
FROM de_duped AS dd
LEFT JOIN stg.stg__provider AS pr
  ON dd.clinician_code = pr.cons_org_code
LEFT JOIN stg.stg_flex__visit_occurrence AS vo
  ON dd.visit_number = vo.visit_number
WHERE
  NOT test IN ('Quadramed req.') AND sequence_id = 1
