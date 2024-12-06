
MODEL (
  name lth_bronze.src_gireport__person,
  kind FULL,
  cron '@daily',
);

select
  mrn,
  person_source_value,
  nhs_number
from @catalaog_src.@schema_src.src_gireport__person
