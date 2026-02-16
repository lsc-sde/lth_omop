MODEL (
  name vcb.vocab__device,
  kind FULL,
  cron '@daily'
);

WITH device_concept AS (
  SELECT
    mpi.person_id AS person_id,
    d.posting_date AS posting_date,
    d.vendor_item_description AS vendor_item_description,
    d.manufacturer_number AS manufacturer_number,
    d.manufacrurer_part_number AS manufacrurer_part_number,
    d.gtin AS gtin,
    d.class_code AS class_code,
    d.lot_no AS lot_no,
    d.serial_no AS serial_no,
    d.expiration_date AS expiration_date,
    d.specialty_code AS specialty_code,
    d.location_code AS location_code,
    d.device_id AS device_id,
    d.snomed_identifier AS snomed_identifier,
    c.concept_id AS device_concept_id,
    c.concept_name AS device_concept_name,
    c.vocabulary_id AS device_vocabulary_id
  FROM stg.stg__device AS d
  LEFT JOIN @catalog_src.@schema_vocab.concept AS c
    ON d.snomed_identifier = c.concept_code AND c.vocabulary_id = 'SNOMED'
  JOIN stg.stg__master_patient_index AS mpi
    ON d.patient_number = mpi.flex_mrn
)
SELECT
  *
FROM device_concept
