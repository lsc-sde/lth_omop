
MODEL (
  name lth_bronze.vocab__device_exposure,
  kind FULL,
  cron '@daily',
);

with devices_base as (
select
  dvc.visit_id,
  vd.visit_detail_id,
  mpi.person_id,
  dvc.date_time as device_datetime,
  manufacturer as device_manufacturer,
  device_type,
  device_type_group,
  device_lot_number,
  device_details,
  expiry_date,
  dvc.source_system::varchar(20),
  dvc.org_code::varchar(5)
from lth_bronze.stg_flex__devices as dvc
left join lth_bronze.stg_flex__visit_detail as vd
  on
    dvc.visit_id = vd.visit_id
    and dvc.date_time >= vd.checkin_datetime
    and dvc.date_time < vd.checkout_datetime
inner join lth_bronze.stg__master_patient_index as mpi
  on dvc.patient_id = mpi.flex_patient_id
  ), 

Devices AS (
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
    END::varchar(75) AS device_source_value,
    db.source_system::varchar(20),
    db.org_code::varchar(5)
  FROM devices_base db
  LEFT JOIN
    (
      SELECT *
      FROM lth_bronze.vocab__source_to_concept_map
      WHERE concept_group = 'devices'
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