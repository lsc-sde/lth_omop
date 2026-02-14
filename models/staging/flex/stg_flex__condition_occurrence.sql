MODEL (
  name lth_bronze.stg_flex__condition_occurrence,
  kind FULL,
  cron '@daily'
);

WITH condition_occ AS (
  SELECT
    visit_number AS visit_number,
    episode_start_dt AS episode_start_dt,
    episode_end_dt AS episode_end_dt,
    index_nbr AS index_nbr,
    provider_source_value AS provider_source_value,
    last_edit_time AS last_edit_time,
    updated_at AS updated_at,
    replace(left(icd10_code, 3) + '.' + substring(icd10_code, 4, 10), '.X', '') AS source_code,
    org_code::VARCHAR(5) AS org_code,
    source_system::VARCHAR(20) AS source_system
  FROM lth_bronze.src_flex__vtg_diagnosis
), visit_detail AS (
  SELECT DISTINCT
    visit_number AS visit_number,
    visit_id AS visit_id,
    first_visit_id AS first_visit_id,
    person_source_value AS person_source_value
  FROM lth_bronze.stg_flex__facility_transfer
)
SELECT
  isnull(v.person_source_value, vs.person_source_value) AS patient_id,
  c.episode_start_dt,
  c.episode_end_dt,
  c.provider_source_value AS provider_id,
  c.source_code,
  c.index_nbr,
  c.last_edit_time,
  c.updated_at,
  isnull(v.first_visit_id, vs.visit_id) AS visit_occurrence_id,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20)
FROM condition_occ AS c
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
    vs.visit_number AS visit_number,
    vs.visit_id AS visit_id,
    vs.person_source_value AS person_source_value
  FROM lth_bronze.src_flex__visit_segment AS vs
  LEFT JOIN visit_detail AS vd
    ON vs.visit_number = vd.visit_number
  WHERE
    vd.visit_number IS NULL
) AS vs
  ON c.visit_number = vs.visit_number AND v.first_visit_id IS NULL
UNION
SELECT
  ae.patient_id,
  activation_time,
  CASE
    WHEN activation_time = discharge_date_time
    THEN discharge_date_time
    ELSE admission_date_time
  END AS condition_end_date,
  provider_id::VARCHAR AS provider_id,
  diagnosis_list,
  NULL,
  last_edit_time,
  updated_at,
  coalesce(v.first_visit_id, ae.visit_id) AS visit_occurrence_id,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20)
FROM lth_bronze.stg_flex__ae_diagnosis AS ae
LEFT JOIN (
  SELECT DISTINCT
    visit_id AS visit_id,
    first_visit_id AS first_visit_id,
    person_source_value AS person_source_value
  FROM visit_detail
) AS v
  ON ae.visit_id = v.visit_id
