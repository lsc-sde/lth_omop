
MODEL (
  name src.src_bi__devices,
  kind VIEW,
  cron '@daily',
);
SELECT
  patient_number::varchar(max) AS patient_number,
  posting_date::varchar(max) AS posting_date,
  vendor_item_description::varchar(max) AS vendor_item_description,
  manufacturer_number::varchar(max) AS manufacturer_number,
  manufacrurer_part_number::varchar(max) AS manufacrurer_part_number,
  gtin::varchar(max) AS gtin,
  class_code::varchar(max) AS class_code,
  lot_no::varchar(max) AS lot_no,
  serial_no::varchar(max) AS serial_no,
  expiration_date::varchar(max) AS expiration_date,
  specialty_code::varchar(max) AS specialty_code,
  location_code::varchar(max) AS location_code,
  device_id::varchar(max) AS device_id
from @catalog_src.@schema_src.src_bi__devices
