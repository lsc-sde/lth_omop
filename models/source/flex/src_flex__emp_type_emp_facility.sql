
MODEL (
  name src.src_flex__emp_type_emp_facility,
  kind VIEW,
  cron '@daily',
);


select
  emp_type_id,
  emp_provider_id,
  facility_id,
  'rxn' as org_code,
  'flex' as source_system
from @catalog_src.@schema_src.src_flex__emp_type_emp_facility
