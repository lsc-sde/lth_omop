
MODEL (
  name src.src_flex__medical_specialty,
  kind VIEW,
  cron '@daily',
);

select
  physician_service_id,
  name,
  facility_id,
  parent_physician_service_id,
  'rxn' as org_code,
  'flex' as source_system
from @catalog_src.@schema_src.src_flex__medical_specialty
