
MODEL (
  name lth_bronze.src_flex__emp_provider,
  kind FULL,
  cron '@daily',
);


select
  name,
  emp_provider_id,
  provider_id
from @catalaog_src.@schema_src.src_flex__emp_provider
