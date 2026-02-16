
MODEL (
  name src.src_flex__emp_facility_med_spec,
  kind VIEW,
  cron '@daily',
);

select
  emp_provider_id,
  facility_id,
  physician_service_id,
  item_nbr,
  'rxn' as org_code,
  'flex' as source_system
from @catalog_src.@schema_src.src_flex__emp_facility_med_spec
