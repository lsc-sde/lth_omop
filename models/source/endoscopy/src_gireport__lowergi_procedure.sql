
MODEL (
  name lth_bronze.src_gireport__lowergi_procedure,
  kind FULL,
  cron '@daily',
);

select
  person_source_value,
  visit_occurrence_id,
  care_site_id,
  procedure_date,
  procedure_datetime,
  provider_id,
  procedure_concept_id,
  procedure_source_value
from @catalaog_src.@schema_src.src_gireport__lowergi_procedure
