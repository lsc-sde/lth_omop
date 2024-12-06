{{
  config(
    materialized = "table",
    tags = ['omop', 'device']
    )
}}

SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY NEWID()) AS bigint) AS device_exposure_id,
  CAST(person_id AS bigint) AS person_id,
  CAST(device_concept_id AS bigint) AS device_concept_id,
  CAST(device_datetime AS date) AS device_exposure_start_date,
  CAST(device_datetime AS datetime) AS device_exposure_start_datetime,
  CAST(NULL AS date) AS device_exposure_end_date,
  CAST(NULL AS datetime) AS device_exposure_end_datetime,
  CAST(32831 AS bigint) AS device_type_concept_id,
  CAST(NULL AS varchar) AS unique_device_id,
  CAST(device_lot_number AS varchar) AS production_id,
  CAST(NULL AS int) AS quantity,
  CAST(NULL AS bigint) AS provider_id,
  CAST(visit_id AS bigint) AS visit_occurrence_id,
  CAST(visit_detail_id AS bigint) AS visit_detail_id,
  CAST(device_source_value AS varchar) AS device_source_value,
  CAST(NULL AS bigint) AS device_source_concept_id,
  CAST(NULL AS bigint) AS unit_concept_id,
  CAST(NULL AS varchar) AS unit_source_value,
  CAST(NULL AS bigint) AS unit_source_concept_id
FROM {{ ref('vocab__device_exposure') }}
