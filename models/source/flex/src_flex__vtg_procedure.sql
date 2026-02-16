
MODEL (
  name src.src_flex__vtg_procedure,
  kind VIEW,
  cron '@daily',
);

select
  visit_number,
  episode_id,
  index_nbr,
  opcs4_code,
  provider_source_value,
  procedure_date,
  'rxn' as org_code,
  'ukcoder' as source_system,
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_flex__vtg_procedure
