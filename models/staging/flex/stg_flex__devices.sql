MODEL (
  name stg.stg_flex__devices,
  kind FULL,
  cron '@daily'
);

WITH devices AS (
  SELECT
    visit_id,
    patient_id,
    date_time,
    manufacturer,
    'catheter' AS device_type_group,
    cath_type AS device_type,
    lot_number AS device_lot_number,
    cath_details AS device_details,
    NULL AS expiry_date,
    source_system::VARCHAR(20),
    org_code::VARCHAR(5)
  FROM src.src_flex__cathether_devices
  UNION ALL
  SELECT
    visit_id,
    patient_id,
    date_time,
    manufacturer,
    theatre_implants AS device_type_group,
    theatre_implants,
    batch_lot_number,
    ammendments,
    expiry_date,
    source_system::VARCHAR(20),
    org_code::VARCHAR(5)
  FROM src.src_flex__implant_devices
), visits AS (
  SELECT DISTINCT
    visit_number AS visit_number,
    visit_id AS visit_id,
    first_visit_id AS first_visit_id,
    person_source_value AS person_source_value
  FROM stg.stg_flex__facility_transfer
)
SELECT
  isnull(v.first_visit_id, d.visit_id) AS visit_id,
  patient_id,
  date_time,
  manufacturer::VARCHAR(150),
  device_type_group::VARCHAR(100),
  device_type::VARCHAR(150),
  device_lot_number::VARCHAR(100),
  device_details::VARCHAR(450),
  expiry_date::VARCHAR(20),
  source_system::VARCHAR(20),
  org_code::VARCHAR(5)
FROM devices AS d
LEFT JOIN visits AS v
  ON d.visit_id = v.visit_id
