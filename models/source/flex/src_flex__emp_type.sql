
MODEL (
  name lth_bronze.src_flex__emp_type,
  kind FULL,
  cron '@daily',
);

select
  emp_type_id,
  name
from @catalaog_src.@schema_src.src_flex__emp_type