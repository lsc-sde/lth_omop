
MODEL (
  name lth_bronze.device_exposure,
  kind FULL,
  cron '@daily',
);

SELECT
  ROW_NUMBER() OVER (ORDER BY NEWID())::bigint AS device_exposure_id,
  person_id::bigint AS person_id,
  device_concept_id::bigint AS device_concept_id,
  device_datetime::date AS device_exposure_start_date,
  device_datetime::datetime AS device_exposure_start_datetime,
  NULL::date AS device_exposure_end_date,
  NULL::datetime AS device_exposure_end_datetime,
  32831::bigint AS device_type_concept_id,
  NULL::varchar AS unique_device_id,
  device_lot_number::varchar(50) AS production_id,
  NULL::int AS quantity,
  NULL::bigint AS provider_id,
  visit_id::bigint AS visit_occurrence_id,
  visit_detail_id::bigint AS visit_detail_id,
  device_source_value::varchar(75) AS device_source_value,
  NULL::bigint AS device_source_concept_id,
  NULL::bigint AS unit_concept_id,
  NULL::varchar AS unit_source_value,
  NULL::bigint AS unit_source_concept_id,
  source_system::varchar(20),
  org_code::varchar(5)
FROM lth_bronze.vocab__device_exposure