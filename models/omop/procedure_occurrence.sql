MODEL (
  name cdm.procedure_occurrence,
  cron '@daily',
  kind FULL
);

WITH src AS (
  SELECT
    @generate_surrogate_key(person_id, visit_occurrence_id, concept_id, procedure_datetime, last_edit_time)::VARBINARY(16) AS procedure_occurrence_id,
    person_id::BIGINT AS person_id,
    concept_id::BIGINT AS procedure_concept_id,
    procedure_date::DATE AS procedure_date,
    procedure_datetime::DATETIME AS procedure_datetime,
    NULL::DATE AS procedure_end_date,
    NULL::DATETIME AS procedure_end_datetime,
    procedure_type_concept_id::BIGINT AS procedure_type_concept_id,
    NULL::BIGINT AS modifier_concept_id,
    1::INTEGER AS quantity,
    provider_id::BIGINT AS provider_id,
    visit_occurrence_id::BIGINT AS visit_occurrence_id,
    NULL::BIGINT AS visit_detail_id,
    procedure_source_value::VARCHAR(50) AS procedure_source_value,
    NULL::BIGINT AS procedure_source_concept_id,
    NULL::VARCHAR(50) AS modifier_source_value,
    org_code::VARCHAR(5) AS org_code,
    source_system::VARCHAR(20) AS source_system,
    last_edit_time::DATETIME AS last_edit_time,
    getdate()::DATETIME AS insert_date_time
  FROM vcb.vocab__procedure_occurrence AS p
), dedup AS (
  SELECT
    *
  FROM (
    SELECT
      src.*,
      row_number() OVER (PARTITION BY procedure_occurrence_id ORDER BY last_edit_time DESC) AS rn
    FROM src
  ) AS t
  WHERE
    rn = 1
)
SELECT
  procedure_occurrence_id,
  person_id,
  procedure_concept_id,
  procedure_date,
  procedure_datetime,
  procedure_end_date,
  procedure_end_datetime,
  procedure_type_concept_id,
  modifier_concept_id,
  quantity,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  procedure_source_value,
  procedure_source_concept_id,
  modifier_source_value,
  org_code,
  source_system,
  last_edit_time,
  insert_date_time
FROM dedup
