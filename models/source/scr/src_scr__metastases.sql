
MODEL (
  name lth_bronze.src_scr__metastases,
  kind FULL,
  cron '@daily',
);

select *
from @catalaog_src.@schema_src.src_scr__metastases
