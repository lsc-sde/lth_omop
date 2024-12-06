
MODEL (
  name lth_bronze.src_scr__trials,
  kind FULL,
  cron '@daily',
);

select *
from @catalog_src.@schema_src.src_scr__trials
