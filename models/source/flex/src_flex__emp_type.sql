
MODEL (
  name src.src_flex__emp_type,
  kind VIEW,
  cron '@daily',
);

select
  emp_type_id,
  name,
  'rxn' as org_code,
  'flex' as source_system
from @catalog_src.@schema_src.src_flex__emp_type
