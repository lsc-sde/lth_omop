MODEL (
  name stg.stg_flex__procedure_occurrence,
  kind FULL,
  cron '@daily'
);

WITH procedure_occ AS (
  SELECT
    visit_number AS visit_number,
    procedure_date AS procedure_date,
    index_nbr AS index_nbr,
    provider_source_value AS provider_source_value,
    org_code AS org_code,
    source_system AS source_system,
    d.last_edit_time AS last_edit_time,
    d.updated_at AS updated_at,
    'ukcoder' AS data_source,
    replace(left(opcs4_code, 3) + '.' + substring(opcs4_code, 4, 10), '.X', '') AS source_code
  FROM src.src_flex__vtg_procedure AS d
), rad_proc_occ AS (
  SELECT
    visit_id AS visit_occurrence_id,
    person_source_value AS person_id,
    CAST((
      event_id / 864000
    ) - 21550 AS DATETIME)::DATETIME2(0) AS procedure_datetime,
    provider_source_value AS provider_id,
    flex_procedure_id AS flex_procedure_id,
    flex_procedure_name AS procedure_source_value,
    org_code AS org_code,
    source_system AS source_system,
    last_edit_time AS last_edit_time,
    updated_at AS updated_at,
    'flex_radiology' AS data_source
  FROM src.src_flex__procedure_event
  WHERE
    kardex_group_id = 24 AND event_status_id IN (6, 11)
), visit_detail AS (
  SELECT DISTINCT
    visit_number AS visit_number,
    visit_id AS visit_id,
    first_visit_id AS first_visit_id,
    person_source_value AS person_source_value
  FROM stg.stg_flex__facility_transfer
)
SELECT
  isnull(v.first_visit_id, vs.visit_id) AS visit_occurrence_id,
  isnull(v.person_source_value, vs.person_source_value) AS person_source_value,
  c.procedure_date AS procedure_date,
  c.procedure_date AS procedure_datetime,
  c.provider_source_value AS provider_id,
  c.source_code AS procedure_source_value,
  c.source_code,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  c.last_edit_time,
  c.updated_at
FROM procedure_occ AS c
LEFT JOIN (
  SELECT DISTINCT
    visit_number AS visit_number,
    first_visit_id AS first_visit_id,
    person_source_value AS person_source_value
  FROM visit_detail
) AS v
  ON c.visit_number = v.visit_number
LEFT JOIN (
  SELECT DISTINCT
    visit_number AS visit_number,
    visit_id AS visit_id,
    person_source_value AS person_source_value
  FROM src.src_flex__visit_segment
  WHERE
    NOT visit_number IN (
      SELECT DISTINCT
        visit_number
      FROM visit_detail
    )
) AS vs
  ON c.visit_number = vs.visit_number AND v.first_visit_id IS NULL
UNION ALL
SELECT
  isnull(v.first_visit_id, c.visit_occurrence_id),
  c.person_id AS person_source_value,
  procedure_datetime::DATE,
  procedure_datetime,
  provider_id::VARCHAR,
  procedure_source_value,
  flex_procedure_id::VARCHAR,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  last_edit_time,
  updated_at
FROM rad_proc_occ AS c
LEFT JOIN visit_detail AS v
  ON c.visit_occurrence_id = v.visit_id
UNION ALL
SELECT
  visit_id AS visit_occurrence_id,
  ae_po.patient_id AS person_source_value,
  procedure_datetime::DATE AS procedure_date,
  procedure_datetime AS procedure_datetime,
  provider_id::VARCHAR,
  NULL AS procedure_source_value,
  ae_po.list AS source_code,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  last_edit_time,
  updated_at
FROM stg.stg_flex__ae_procedures AS ae_po
