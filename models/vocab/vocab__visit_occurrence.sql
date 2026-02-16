MODEL (
  name vcb.vocab__visit_occurrence,
  kind FULL,
  cron '@daily'
);

WITH visits AS (
  SELECT
    mpi.person_id AS person_id,
    fvo.visit_number AS visit_number,
    fvo.activation_time AS activation_time,
    fvo.visit_type_id AS visit_type_id,
    fvo.visit_status_id AS visit_status_id,
    fvo.visit_id AS visit_id,
    fvo.admission_time AS admission_time,
    fvo.discharge_time AS discharge_time,
    fvo.provider_id AS provider_id,
    fvo.facility_id AS facility_id,
    fvo.admitted_from_source_value AS admitted_from_source_value,
    fvo.discharged_to_source_value AS discharged_to_source_value,
    fvo.source_system AS source_system,
    fvo.org_code AS org_code,
    fvo.last_edit_time AS last_edit_time,
    fvo.updated_at AS updated_at
  FROM stg.stg_flex__visit_occurrence AS fvo
  INNER JOIN stg.stg__master_patient_index AS mpi
    ON fvo.person_source_value = mpi.flex_patient_id
  WHERE
    NOT fvo.visit_type_id IS NULL
), ae_visits AS (
  SELECT DISTINCT
    visit_id AS visit_id
  FROM src.src_flex__visit_detail_ae
)
SELECT
  vo.visit_id AS visit_occurrence_id,
  vo.person_id,
  CASE
    WHEN vo.visit_type_id IN (3, 4)
    THEN isnull(discharge_time, admission_time)
    WHEN vo.visit_status_id IN (7)
    THEN isnull(discharge_time, admission_time)
    ELSE discharge_time
  END AS visit_end_datetime,
  32817 AS visit_type_concept_id,
  provider_id,
  facility_id AS care_site_id,
  NULL AS visit_source_concept_id,
  vo.admitted_from_source_value,
  vo.discharged_to_source_value,
  CASE
    WHEN vo.visit_type_id IN (1, 6) AND NOT a.visit_id IS NULL
    THEN 262
    WHEN vo.visit_type_id IN (1, 6)
    THEN 9201
    WHEN vo.visit_type_id = 2
    THEN 9203
    WHEN vo.visit_type_id IN (3, 4)
    THEN 9202
    WHEN vo.visit_type_id IN (5, 8, 7, 9)
    THEN 32761
  END AS visit_concept_id,
  CASE
    WHEN vo.visit_type_id IN (1, 6) AND NOT a.visit_id IS NULL
    THEN activation_time
    ELSE admission_time
  END AS visit_start_datetime,
  CASE
    WHEN vo.visit_type_id IN (1, 6) AND NOT a.visit_id IS NULL
    THEN 'ERIP'
    WHEN vo.visit_type_id IN (1, 6)
    THEN 'IP'
    WHEN vo.visit_type_id = 2
    THEN 'ER'
    WHEN vo.visit_type_id IN (3, 4)
    THEN 'OP'
    WHEN vo.visit_type_id IN (5, 8, 7, 9)
    THEN 'PUI'
    ELSE 'ER'
  END AS visit_source_value,
  CASE WHEN csa.target_concept_id = '4139502' THEN 0 ELSE csa.target_concept_id END AS admitted_from_concept_id,
  CASE WHEN csd.target_concept_id = '4139502' THEN NULL ELSE csd.target_concept_id END AS discharged_to_concept_id,
  vo.visit_status_id,
  vo.visit_type_id,
  vo.source_system::VARCHAR(20),
  vo.org_code::VARCHAR(5),
  vo.last_edit_time,
  vo.updated_at
FROM visits AS vo
LEFT JOIN (
  SELECT
    target_concept_id AS target_concept_id,
    source_code AS source_code
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'discharge_destination'
) AS csd
  ON vo.discharged_to_source_value = csd.source_code
LEFT JOIN (
  SELECT
    target_concept_id AS target_concept_id,
    source_code AS source_code
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'admission_source'
) AS csa
  ON vo.admitted_from_source_value = csa.source_code
LEFT JOIN ae_visits AS a
  ON vo.visit_id = a.visit_id
