
MODEL (
  name lth_bronze.src_flex__emp_type,
  kind VIEW,
  cron '@daily',
);

select
  emp_type_id,
  name
from @catalog_src.@schema_src.src_flex__emp_type