MODEL (
  name vcb.vocab__visit_detail,
  kind FULL,
  cron '@daily'
);

WITH visit_detail AS (
  SELECT
    vd.visit_detail_id::NUMERIC AS visit_detail_id,
    vd.visit_id AS visit_id,
    mpi.person_id AS person_id,
    visit_type AS visit_type,
    location_id AS location_id,
    vd.checkin_datetime AS checkin_datetime,
    vd.checkout_datetime AS checkout_datetime,
    vd.last_edit_time AS last_edit_time,
    vd.source_system AS source_system,
    vd.org_code AS org_code,
    updated_at AS updated_at
  FROM stg.stg_flex__visit_detail AS vd
  INNER JOIN stg.stg__master_patient_index AS mpi
    ON mpi.flex_patient_id = vd.patient_id
)
SELECT
  vd.visit_detail_id,
  vd.person_id,
  vd.checkin_datetime AS visit_detail_start_datetime,
  vd.checkout_datetime AS visit_detail_end_datetime,
  32817 AS visit_detail_type_concept_id,
  NULL AS provider_id,
  vd.visit_type AS visit_detail_source_value,
  NULL AS visit_detail_source_concept_id,
  NULL AS admitted_from_concept_id,
  NULL AS admitted_from_source_value,
  NULL AS discharged_to_source_value,
  NULL AS discharged_to_concept_id,
  NULL AS parent_visit_detail_id,
  vd.visit_id AS visit_occurrence_id,
  CASE
    WHEN vd.visit_type = 'ER'
    THEN 9203
    WHEN vd.visit_type = 'ERIP'
    THEN 262
    WHEN vd.visit_type = 'IP'
    THEN 9201
  END AS visit_detail_concept_id,
  left(vd.location_id, charindex('-', vd.location_id + '-') - 1) AS location_id,
  lag(vd.visit_detail_id) OVER (PARTITION BY vd.person_id ORDER BY vd.checkin_datetime) AS preceding_visit_detail_id,
  source_system::VARCHAR(20),
  org_code::VARCHAR(5)
FROM visit_detail AS vd
