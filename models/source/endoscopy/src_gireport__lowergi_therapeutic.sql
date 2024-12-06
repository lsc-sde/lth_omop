
MODEL (
  name lth_bronze.src_gireport__lowergi_therapeutic,
  kind FULL,
  cron '@daily',
);

select
  person_source_value,
  visit_occurrence_id,
  anatomic_site_source_value,
  parent_procedure_source_value,
  procedure_source_value
from @catalog_src.@schema_src.src_gireport__lowergi_therapeutic