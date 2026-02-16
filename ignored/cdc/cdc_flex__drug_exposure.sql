MODEL (
  name lth_bronze.cdc_flex__drug_exposure,
  kind FULL,
  cron '@daily',
  enabled (
    1 = 0
  )
);

WITH cdc AS (
  SELECT
    min(updated_at) AS updated_at
  FROM lth_bronze.cdc__updated_at
  WHERE
    model IN ('DRUG_EXPOSURE') AND datasource = 'flex'
)
SELECT
  visit_id AS visit_occurrence_id,
  person_source_value,
  CAST((
    event_id / 864000
  ) - 21550 AS DATETIME)::DATETIME2(0) AS event_datetime,
  provider_source_value AS provider_id,
  flex_procedure_id,
  flex_procedure_name AS procedure_source_value,
  adm_route,
  source_system,
  org_code,
  last_edit_time,
  updated_at,
  replace(dosage, ' ' + adm_route, '') AS dosage
FROM lth_bronze.src_flex__procedure_event AS sfr
WHERE
  sfr.last_edit_time > (
    SELECT
      updated_at
    FROM cdc
  )
  AND sfr.last_edit_time < (
    SELECT
      dateadd(DAY, 365, updated_at)
    FROM cdc
  )
  AND sfr.last_edit_time <= getdate()
  AND kardex_group_id IN (17, 43, 44)
  AND event_status_id IN (6, 11)
