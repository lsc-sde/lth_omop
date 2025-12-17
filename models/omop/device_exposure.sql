MODEL (
  name lth_bronze.device_exposure,
  kind FULL,
  cron '@daily',
);

WITH ve AS (
  -- Existing curated device exposure rows
  SELECT
    person_id::bigint AS person_id,
    device_concept_id::bigint AS device_concept_id,
    device_datetime::datetime AS device_datetime,
    device_lot_number::varchar(50) AS device_lot_number,
    visit_id::bigint AS visit_id,
    visit_detail_id::bigint AS visit_detail_id,
    device_source_value::varchar(75) AS device_source_value,
    NULL::varchar(50) AS unique_device_id, -- keep null to match existing output
    32831::bigint AS device_type_concept_id,
    NULL::bigint AS provider_id,
    NULL::bigint AS device_source_concept_id,
    NULL::bigint AS unit_concept_id,
  NULL::varchar(100) AS unit_source_value,
    NULL::bigint AS unit_source_concept_id,
    source_system::varchar(20) AS source_system,
    org_code::varchar(5) AS org_code
  FROM lth_bronze.vocab__device_exposure
  WHERE device_concept_id IS NOT NULL
),
vd AS (
  -- Additional rows sourced from vocab__device (SNOMED-joined)
  -- Adjust column names here if they differ in lth_bronze.vocab__device
  SELECT
    person_id::bigint AS person_id,
  device_concept_id::bigint AS device_concept_id,
    TRY_CAST(posting_date AS datetime) AS device_datetime,
    lot_no::varchar(50) AS device_lot_number,
    NULL::bigint AS visit_id,
    NULL::bigint AS visit_detail_id,
    COALESCE(vendor_item_description, device_id)::varchar(75) AS device_source_value,
  device_id::varchar(100) AS unique_device_id,
    32831::bigint AS device_type_concept_id,
    NULL::bigint AS provider_id,
    NULL::bigint AS device_source_concept_id,
    NULL::bigint AS unit_concept_id,
  NULL::varchar(100) AS unit_source_value,
    NULL::bigint AS unit_source_concept_id,
    'Atticus'::varchar(20) AS source_system,
    'RXN'::varchar(5) AS org_code
  FROM lth_bronze.vocab__device
  WHERE device_concept_id IS NOT NULL
),
unioned AS (
  SELECT * FROM ve
  UNION ALL
  SELECT * FROM vd
)
SELECT
  abs(cast(cast(
    @generate_surrogate_key(
      person_id,
      visit_id,
      device_concept_id,
      COALESCE(unique_device_id, device_lot_number, device_source_value),
      device_datetime,
      org_code,
      source_system
    )
  as varbinary(8)) as bigint)) AS device_exposure_id,
  person_id,
  device_concept_id,
  device_datetime::date AS device_exposure_start_date,
  device_datetime::datetime AS device_exposure_start_datetime,
  NULL::date AS device_exposure_end_date,
  NULL::datetime AS device_exposure_end_datetime,
  device_type_concept_id,
  unique_device_id,
  device_lot_number AS production_id,
  NULL::int AS quantity,
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
FROM unioned;