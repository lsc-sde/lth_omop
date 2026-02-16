MODEL (
  name stg.stg__device,
  kind VIEW
);

SELECT
  d.patient_number,
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
  c.snomed_identifier
FROM src.src_bi__devices AS d
JOIN stg.stg__device_cache AS c
  ON c.device_id = d.device_id
