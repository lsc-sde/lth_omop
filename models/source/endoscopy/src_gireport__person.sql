
MODEL (
  name lth_bronze.src_gireport__person,
  kind VIEW,
  cron '@daily',
);

select
  mrn,
  person_source_value,
  nhs_number,
  'rxn' as org_code,
  'endoscopy' as source_system
from @catalog_src.@schema_src.src_gireport__person
