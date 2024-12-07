
MODEL (
  name lth_bronze.src_gireport__person,
  kind VIEW,
  cron '@daily',
);

select
  mrn,
  person_source_value,
  nhs_number
from @catalog_src.@schema_src.src_gireport__person
