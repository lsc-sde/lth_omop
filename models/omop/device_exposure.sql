MODEL (
  name lth_bronze.device_exposure,
  kind FULL,
  cron '@daily'
);

WITH ve AS (
  /* Existing curated device exposure rows */
  SELECT
    person_id::BIGINT AS person_id,
    device_concept_id::BIGINT AS device_concept_id,
    device_datetime::DATETIME AS device_datetime,
    device_lot_number::VARCHAR(50) AS device_lot_number,
    visit_id::BIGINT AS visit_id,
    visit_detail_id::BIGINT AS visit_detail_id,
    device_source_value::VARCHAR(75) AS device_source_value,
    NULL::VARCHAR(50) AS unique_device_id, /* keep null to match existing output */
    32831::BIGINT AS device_type_concept_id,
    NULL::BIGINT AS provider_id,
    NULL::BIGINT AS device_source_concept_id,
    NULL::BIGINT AS unit_concept_id,
    NULL::VARCHAR(100) AS unit_source_value,
    NULL::BIGINT AS unit_source_concept_id,
    source_system::VARCHAR(20) AS source_system,
    org_code::VARCHAR(5) AS org_code
  FROM lth_bronze.vocab__device_exposure
  WHERE
    NOT device_concept_id IS NULL
), vd AS (
  /* Additional rows sourced from vocab__device (SNOMED-joined) */ /* Adjust column names here if they differ in lth_bronze.vocab__device */
  SELECT
    person_id::BIGINT AS person_id,
    device_concept_id::BIGINT AS device_concept_id,
    TRY_CAST(posting_date AS DATETIME) AS device_datetime,
    lot_no::VARCHAR(50) AS device_lot_number,
    NULL::BIGINT AS visit_id,
    NULL::BIGINT AS visit_detail_id,
    coalesce(vendor_item_description, device_id)::VARCHAR(75) AS device_source_value,
    device_id::VARCHAR(100) AS unique_device_id,
    32831::BIGINT AS device_type_concept_id,
    NULL::BIGINT AS provider_id,
    NULL::BIGINT AS device_source_concept_id,
    NULL::BIGINT AS unit_concept_id,
    NULL::VARCHAR(100) AS unit_source_value,
    NULL::BIGINT AS unit_source_concept_id,
    'Atticus'::VARCHAR(20) AS source_system,
    'RXN'::VARCHAR(5) AS org_code
  FROM lth_bronze.vocab__device
  WHERE
    NOT device_concept_id IS NULL
), unioned AS (
  SELECT
    *
  FROM ve
  UNION ALL
  SELECT
    *
  FROM vd
)
SELECT
  @generate_surrogate_key(
    person_id,
    visit_id,
    device_concept_id,
    coalesce(unique_device_id, device_lot_number, device_source_value),
    device_datetime,
    org_code,
    source_system
  )::VARBINARY(16) AS device_exposure_id,
  person_id,
  device_concept_id,
  device_datetime::DATE AS device_exposure_start_date,
  device_datetime::DATETIME AS device_exposure_start_datetime,
  NULL::DATE AS device_exposure_end_date,
  NULL::DATETIME AS device_exposure_end_datetime,
  device_type_concept_id,
  unique_device_id,
  device_lot_number AS production_id,
  NULL::INTEGER AS quantity,
  provider_id,
  visit_id AS visit_occurrence_id,
  visit_detail_id,
  device_source_value,
  device_source_concept_id,
  unit_concept_id,
  unit_source_value,
  unit_source_concept_id,
  source_system,
  org_code
FROM unioned
