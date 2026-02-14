MODEL (
  name lth_bronze.stg__result,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at,
    batch_size 30,
    batch_concurrency 4
  ),
  cron '@daily'
);

WITH results_union AS (
  SELECT
    flex_patient_id AS patient_id,
    visit_occurrence_id,
    measurement_event_id::VARCHAR(50) AS measurement_event_id,
    result_datetime,
    provider_id,
    TRY_CAST(source_code AS VARCHAR(50)) AS source_code,
    TRY_CAST(source_name AS VARCHAR) AS source_name,
    TRY_CAST(value_source_value AS VARCHAR) AS value_source_value,
    TRY_CAST(source_value AS VARCHAR) AS source_value,
    value_as_number,
    TRY_CAST(unit_source_value AS VARCHAR) AS unit_source_value,
    TRY_CAST(NULL AS VARCHAR) AS priority,
    org_code,
    source_system,
    updated_at
  FROM lth_bronze.stg_flex__result
  WHERE
    updated_at BETWEEN @start_ds AND @end_ds
  UNION ALL
  SELECT
    bi_patient_id AS patient_id,
    visit_occurrence_id,
    measurement_event_id::VARCHAR(50) AS measurement_event_id,
    bi.referral_received_date AS result_datetime,
    provider_id,
    TRY_CAST(source_code AS VARCHAR(50)) AS source_code,
    TRY_CAST(source_name AS VARCHAR) AS source_name,
    TRY_CAST(value_source_value AS VARCHAR) AS value_source_value,
    TRY_CAST(source_value AS VARCHAR) AS source_value,
    value_as_number,
    TRY_CAST(unit_source_value AS VARCHAR) AS unit_source_value,
    TRY_CAST(priority AS VARCHAR) AS priority,
    org_code,
    source_system,
    updated_at
  FROM lth_bronze.stg_bi__referrals AS bi
  WHERE
    updated_at BETWEEN @start_ds AND @end_ds
  UNION ALL
  SELECT
    TRY_CAST(nhs_number AS NUMERIC) AS patient_id,
    visit_occurrence_id,
    measurement_event_id::VARCHAR(50) AS measurement_event_id,
    order_date AS result_datetime,
    provider_id,
    TRY_CAST(source_code AS VARCHAR(50)) AS source_code,
    TRY_CAST(source_name AS VARCHAR) AS source_name,
    TRY_CAST(value_source_value AS VARCHAR) AS value_source_value,
    TRY_CAST(source_value AS VARCHAR) AS source_value,
    value_as_number,
    TRY_CAST(unit_source_value AS VARCHAR) AS unit_source_value,
    TRY_CAST(priority AS VARCHAR) AS priority,
    org_code,
    source_system,
    updated_at
  FROM lth_bronze.cdc_sl__bacteriology AS ssb
  WHERE
    ssb.updated_at BETWEEN @start_ds AND @end_ds AND ssb.valid_to IS NULL
), person AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY person_id ORDER BY last_edit_time DESC) AS id
  FROM lth_bronze.stg__master_patient_index
)
SELECT
  coalesce(mpi.person_id, mpi_2.person_id) AS person_id,
  visit_occurrence_id AS visit_occurrence_id,
  ru.measurement_event_id::VARCHAR(50) AS measurement_event_id,
  ru.provider_id AS provider_id,
  ru.result_datetime AS result_datetime,
  ru.value_as_number AS value_as_number,
  ru.source_name::VARCHAR(200) AS source_name,
  ru.source_code AS source_code,
  ru.unit_source_value::VARCHAR(200) AS unit_source_value,
  ru.value_source_value::VARCHAR(200) AS value_source_value,
  ru.source_value::VARCHAR(200) AS source_value,
  ru.priority::VARCHAR(200) AS priority,
  ru.org_code::VARCHAR(200) AS org_code,
  ru.source_system::VARCHAR(200) AS source_system,
  updated_at AS updated_at
FROM results_union AS ru
LEFT JOIN person AS mpi
  ON ru.patient_id = mpi.flex_patient_id
  AND ru.source_system IN ('flex', 'bi')
  AND mpi.id = 1
LEFT JOIN (
  SELECT DISTINCT
    person_id AS person_id,
    nhs_number AS nhs_number
  FROM person
) AS mpi_2
  ON ru.patient_id = mpi_2.nhs_number AND ru.source_system IN ('swl')
WHERE
  NOT person_id IS NULL AND updated_at BETWEEN @start_ds AND @end_ds
