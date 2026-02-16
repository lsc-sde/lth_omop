
MODEL (
  name src.src_flex__vtg_diagnosis,
  kind VIEW,
  cron '@daily',
);

select
  visit_number,
  episode_id,
  index_nbr,
  icd10_code,
  provider_source_value,
  episode_start_dt,
  episode_end_dt,
  'rxn' as org_code,
  'ukcoder' as source_system,
  last_edit_time,
  updated_at
from @catalog_src.@schema_src.src_flex__vtg_diagnosis
