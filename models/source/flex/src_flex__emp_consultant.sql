
MODEL (
  name lth_bronze.src_flex__emp_consultant,
  kind VIEW,
  cron '@daily',
);

select
cons_org_code,
cons_emp_provider_id,
cons_provider
from @catalog_src.@schema_src.src_flex__emp_consultant
