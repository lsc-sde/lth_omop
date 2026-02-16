
MODEL (
  name src.src_flex__emp_provider,
  kind VIEW,
  cron '@daily',
);


select
  name,
  emp_provider_id,
  provider_id,
  'rxn' as org_code,
  'flex' as source_system
from @catalog_src.@schema_src.src_flex__emp_provider
