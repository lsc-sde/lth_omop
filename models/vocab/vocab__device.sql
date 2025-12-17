MODEL (
  name lth_bronze.vocab__device,
  kind FULL,
  cron '@daily',
);

with device_concept as (
  select
    mpi.person_id,
    d.posting_date,
    d.vendor_item_description,
    d.manufacturer_number,
    d.manufacrurer_part_number,
    d.gtin,
    d.class_code,
    d.lot_no,
    d.serial_no,
    d.expiration_date,
    d.specialty_code,
    d.location_code,
    d.device_id,
    d.snomed_identifier,
    c.concept_id as device_concept_id,
    c.concept_name as device_concept_name,
    c.vocabulary_id as device_vocabulary_id
  from lth_bronze.stg__device as d
  left join @catalog_src.@schema_vocab.concept as c
    on d.snomed_identifier = c.concept_code
    and c.vocabulary_id = 'SNOMED'
  join lth_bronze.stg__master_patient_index mpi
    on d.patient_number = mpi.flex_mrn
)

select * from device_concept
