MODEL (
  name lth_bronze.stg_flex__drug_exposure,
  kind VIEW,
  cron '@daily'
);

WITH visit_detail AS (
  SELECT DISTINCT
    visit_number AS visit_number,
    visit_id AS visit_id,
    first_visit_id AS first_visit_id,
    person_source_value AS person_source_value
  FROM lth_bronze.stg_flex__facility_transfer
)
SELECT
  sfr.person_source_value,
  CAST((
    event_id / 864000
  ) - 21550 AS DATETIME)::DATETIME2(0) AS drug_exposure_start_datetime,
  CAST((
    event_id / 864000
  ) - 21550 AS DATETIME)::DATETIME2(0)::DATE AS drug_exposure_date,
  provider_source_value::VARCHAR AS provider_id,
  flex_procedure_id,
  replace(dosage, ' ' + adm_route, '') AS dosage,
  adm_route,
  source_system::VARCHAR(20),
  org_code::VARCHAR(5),
  last_edit_time,
  updated_at,
  replace(flex_procedure_name, ' (CRITICAL MED)', '') AS drug_source_value,
  isnull(v.first_visit_id, sfr.visit_id) AS visit_occurrence_id
FROM lth_bronze.src_flex__procedure_event AS sfr
LEFT JOIN visit_detail AS v
  ON sfr.visit_id = v.visit_id
WHERE
  sfr.last_edit_time <= getdate()
  AND sfr.kardex_group_id IN (17, 43, 44)
  AND sfr.event_status_id IN (6, 11)
