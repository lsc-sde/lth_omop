
MODEL (
  name lth_bronze.src_gireport__diagnostic_codes,
  kind VIEW,
  cron '@daily',
);

select
  table_name,
  field_name,
  condition_source_value
from @catalog_src.@schema_src.src_gireport__diagnostic_codes
