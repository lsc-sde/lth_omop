
MODEL (
  name lth_bronze.src_gireport__uppergi_diagnosis,
  kind VIEW,
  cron '@daily',
);

select
  person_source_value,
  visit_occurrence_id,
  condition_source_value
from @catalog_src.@schema_src.src_gireport__uppergi_diagnosis