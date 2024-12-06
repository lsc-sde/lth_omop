
MODEL (
  name lth_bronze.src_gireport__condition_occurrence,
  kind FULL,
  cron '@daily',
);

select
  person_source_value,
  visit_occurrence_id,
  condition_source_value
from @catalaog_src.@schema_src.src_gireport__condition_occurrence