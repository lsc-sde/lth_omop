MODEL (
  name stg.stg_flex__visit_detail,
  kind FULL,
  cron '@daily'
);

WITH ip_detail AS (
  SELECT
    patient_id AS patient_id,
    visit_id AS visit_id,
    visit_number AS visit_number,
    location_id AS location_id,
    location_hx_time AS checkin_datetime,
    coalesce(
      lead(location_hx_time) OVER (PARTITION BY visit_id ORDER BY location_hx_time),
      discharge_date_time
    ) AS checkout_datetime,
    source_system AS source_system,
    org_code AS org_code,
    last_edit_time AS last_edit_time,
    updated_at AS updated_at
  FROM src.src_flex__visit_detail_ip
), ae_detail AS (
  SELECT
    patient_id AS patient_id,
    visit_id AS visit_id,
    visit_number AS visit_number,
    location_id AS location_id,
    date_time_in AS checkin_datetime,
    date_time_out AS checkout_datetime,
    source_system AS source_system,
    org_code AS org_code,
    last_edit_time AS last_edit_time,
    updated_at AS updated_at
  FROM src.src_flex__visit_detail_ae
)
SELECT
  a.patient_id::BIGINT,
  a.visit_number::VARCHAR(50),
  visit_type::VARCHAR(10),
  location_id::VARCHAR(50),
  checkin_datetime::DATETIME,
  checkout_datetime::DATETIME,
  a.source_system::VARCHAR(20),
  a.org_code::VARCHAR(5),
  a.last_edit_time::DATETIME,
  a.updated_at::DATETIME,
  concat(
    coalesce(ft.first_visit_id, a.visit_id),
    row_number() OVER (PARTITION BY coalesce(ft.first_visit_id, a.visit_id) ORDER BY checkin_datetime)
  )::BIGINT AS visit_detail_id,
  coalesce(ft.first_visit_id, a.visit_id) AS visit_id
FROM (
  SELECT
    ae.*,
    CASE WHEN NOT ip.visit_id IS NULL THEN 'ERIP' ELSE 'ER' END AS visit_type
  FROM ae_detail AS ae
  LEFT JOIN (
    SELECT DISTINCT
      visit_id AS visit_id
    FROM ip_detail
  ) AS ip
    ON ip.visit_id = ae.visit_id
  UNION ALL
  SELECT
    ip.*,
    CASE WHEN NOT ae.visit_id IS NULL THEN 'ERIP' ELSE 'IP' END AS visit_type
  FROM ip_detail AS ip
  LEFT JOIN (
    SELECT DISTINCT
      visit_id AS visit_id
    FROM ae_detail
  ) AS ae
    ON ip.visit_id = ae.visit_id
) AS a
LEFT JOIN stg.stg_flex__facility_transfer AS ft
  ON a.visit_id = ft.visit_id
