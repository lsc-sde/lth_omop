MODEL (
  name vcb.vocab__device_exposure,
  kind FULL,
  cron '@daily'
);

WITH devices_base AS (
  SELECT
    dvc.visit_id AS visit_id,
    vd.visit_detail_id AS visit_detail_id,
    mpi.person_id AS person_id,
    dvc.date_time AS device_datetime,
    manufacturer AS device_manufacturer,
    device_type AS device_type,
    device_type_group AS device_type_group,
    device_lot_number AS device_lot_number,
    device_details AS device_details,
    expiry_date AS expiry_date,
    dvc.source_system::VARCHAR(20) AS source_system,
    dvc.org_code::VARCHAR(5) AS org_code
  FROM stg.stg_flex__devices AS dvc
  LEFT JOIN stg.stg_flex__visit_detail AS vd
    ON dvc.visit_id = vd.visit_id
    AND dvc.date_time >= vd.checkin_datetime
    AND dvc.date_time < vd.checkout_datetime
  INNER JOIN stg.stg__master_patient_index AS mpi
    ON dvc.patient_id = mpi.flex_patient_id
), devices AS (
  SELECT
    visit_id AS visit_id,
    visit_detail_id AS visit_detail_id,
    CASE
      WHEN device_details LIKE '%mesh%' AND cm.target_concept_id IS NULL
      THEN '4223318'
      WHEN device_details LIKE '%nail%' AND cm.target_concept_id IS NULL
      THEN '4272781'
      WHEN device_details LIKE '%screw%' AND cm.target_concept_id IS NULL
      THEN '45768004'
      ELSE cm.target_concept_id
    END AS device_concept_id,
    person_id AS person_id,
    device_datetime AS device_datetime,
    device_manufacturer AS device_manufacturer,
    device_type AS device_type,
    device_type_group AS device_type_group,
    device_lot_number AS device_lot_number,
    device_details AS device_details,
    expiry_date AS expiry_date,
    CASE
      WHEN device_details LIKE '%mesh%' AND cm.target_concept_id IS NULL
      THEN device_details
      WHEN device_details LIKE '%screw%' AND cm.target_concept_id IS NULL
      THEN device_details
      WHEN device_type_group = 'catheter' AND NOT device_type IS NULL
      THEN device_type + ' ' + device_type_group
      WHEN device_type_group = 'catheter'
      THEN device_type_group
      ELSE isnull(device_type, device_details)
    END::VARCHAR(75) AS device_source_value,
    db.source_system::VARCHAR(20) AS source_system,
    db.org_code::VARCHAR(5) AS org_code
  FROM devices_base AS db
  LEFT JOIN (
    SELECT
      *
    FROM vcb.vocab__source_to_concept_map
    WHERE
      concept_group = 'devices'
  ) AS cm
    ON CASE
      WHEN device_type_group = 'catheter' AND NOT device_type IS NULL
      THEN device_type + ' ' + device_type_group
      WHEN device_type_group = 'catheter'
      THEN 'catheter'
      ELSE isnull(device_type, device_details)
    END = cm.source_code
)
SELECT
  *
FROM devices
WHERE
  NOT device_concept_id IS NULL
  AND (
    NOT device_details IN ('in place on arrival') OR device_details IS NULL
  )
