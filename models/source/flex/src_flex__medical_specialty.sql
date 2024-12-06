
MODEL (
  name lth_bronze.src_flex__medical_specialty,
  kind FULL,
  cron '@daily',
);

select
  physician_service_id,
  name,
  facility_id,
  parent_physician_service_id
from @catalog_src.@schema_src.src_flex__medical_specialty