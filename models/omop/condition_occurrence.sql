MODEL (
  name cdm.condition_occurrence,
  cron '@daily',
  kind FULL
);

WITH cons_provider AS (
  SELECT
    *
  FROM vcb.vocab__provider
  WHERE
    NOT cons_org_code IS NULL
), src AS (
  SELECT
    @generate_surrogate_key(
      person_id,
      c.provider_id,
      visit_occurrence_id,
      condition_concept_id,
      condition_status_concept_id,
      condition_start_date,
      condition_end_date,
      condition_source_value,
      c.last_edit_time
    )::VARBINARY(16) AS condition_occurrence_id,
    person_id::BIGINT AS person_id,
    condition_concept_id::BIGINT AS condition_concept_id,
    condition_start_date::DATE AS condition_start_date,
    condition_start_date::DATETIME AS condition_start_datetime,
    condition_end_date::DATE AS condition_end_date,
    condition_end_date::DATETIME AS condition_end_datetime,
    condition_type_concept_id::BIGINT AS condition_type_concept_id,
    condition_status_concept_id::BIGINT AS condition_status_concept_id,
    NULL::VARCHAR(20) AS stop_reason,
    isnull(pr1.provider_id, pr2.provider_id)::BIGINT AS provider_id,
    visit_occurrence_id::BIGINT AS visit_occurrence_id,
    NULL::BIGINT AS visit_detail_id,
    condition_source_value::VARCHAR(50) AS condition_source_value,
    condition_source_concept_id::BIGINT AS condition_source_concept_id,
    NULL::VARCHAR(50) AS condition_status_source_value,
    c.org_code AS org_code,
    c.source_system AS source_system,
    c.last_edit_time AS last_edit_time,
    c.updated_at AS updated_at,
    getdate() AS insert_date_time
  FROM vcb.vocab__condition_occurrence AS c
  LEFT JOIN cons_provider AS pr1
    ON c.provider_id = pr1.cons_org_code AND c.provider_id_type = 0
  LEFT JOIN vcb.vocab__provider AS pr2
    ON c.provider_id = pr2.provider_source_value AND c.provider_id_type = 1
), dedup AS (
  SELECT
    *
  FROM (
    SELECT
      src.*,
      row_number() OVER (
        PARTITION BY condition_occurrence_id
        ORDER BY coalesce(updated_at, last_edit_time) DESC
      ) AS rn
    FROM src
  ) AS t
  WHERE
    rn = 1
)
SELECT
  condition_occurrence_id,
  person_id,
  condition_concept_id,
  condition_start_date,
  condition_start_datetime,
  condition_end_date,
  condition_end_datetime,
  condition_type_concept_id,
  condition_status_concept_id,
  stop_reason,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  condition_source_value,
  condition_source_concept_id,
  condition_status_source_value,
  org_code,
  source_system,
  last_edit_time,
  updated_at,
  insert_date_time
FROM dedup
