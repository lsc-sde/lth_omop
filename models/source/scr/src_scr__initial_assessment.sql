
MODEL (
  name lth_bronze.src_scr__initial_assessment,
  kind VIEW,
  cron '@daily',
);

select *
from @catalog_src.@schema_src.src_scr__initial_assessments
