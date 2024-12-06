
MODEL (
  name lth_bronze.vocab__device_exposure,
  kind FULL,
  cron '@daily',
);

WITH Devices AS (
  SELECT
    visit_id,
    visit_detail_id,
    CASE
      WHEN
        device_details LIKE '%mesh%' AND cm.target_concept_id IS NULL
        THEN '4223318'
      WHEN
        device_details LIKE '%nail%' AND cm.target_concept_id IS NULL
        THEN '4272781'
      WHEN
        device_details LIKE '%screw%' AND cm.target_concept_id IS NULL
        THEN '45768004'
      ELSE cm.target_concept_id
    END AS device_concept_id,
    person_id,
    device_datetime,
    device_manufacturer,
    device_type,
    device_type_group,
    device_lot_number,
    device_details,
    expiry_date,
    CASE
      WHEN
        device_details LIKE '%mesh%' AND cm.target_concept_id IS NULL
        THEN device_details
      WHEN
        device_details LIKE '%screw%' AND cm.target_concept_id IS NULL
        THEN device_details
      WHEN device_type_group = 'catheter' AND device_type IS NOT NULL
        THEN device_type + ' ' + device_type_group
      WHEN device_type_group = 'catheter' THEN device_type_group
      ELSE isnull(device_type, device_details)
    END AS device_source_value
  FROM lth_bronze.stg__device_exposure 
  LEFT JOIN
    (
      SELECT *
      FROM lth_bronze.vocab__source_to_concept_map 
      WHERE [group] = 'devices'
    ) AS cm
    ON CASE
      WHEN
        device_type_group = 'catheter' AND device_type IS NOT NULL
        THEN device_type + ' ' + device_type_group
      WHEN device_type_group = 'catheter' THEN 'catheter'
      ELSE isnull(device_type, device_details)
    END = cm.source_code
)

SELECT * FROM
  Devices WHERE device_concept_id IS NOT NULL
AND (device_details NOT IN ('in place on arrival') OR device_details IS NULL)
