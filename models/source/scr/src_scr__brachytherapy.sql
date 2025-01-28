
MODEL (
  name lth_bronze.src_scr__brachytherapy,
  kind VIEW,
  cron '@daily',
);

select *,
  'rxn' as org_code,
  'scr' as source_system
from @catalog_src.@schema_src.src_scr__brachytherapy
