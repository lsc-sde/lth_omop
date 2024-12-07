
MODEL (
  name lth_bronze.src_scr__breast_markers,
  kind VIEW,
  cron '@daily',
);

select *
from @catalog_src.@schema_src.src_scr__breast_markers
