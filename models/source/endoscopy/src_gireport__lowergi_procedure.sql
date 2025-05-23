
MODEL (
  name lth_bronze.src_gireport__lowergi_procedure,
  kind VIEW,
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
  procedure_source_value,
  'rxn' as org_code,
  'endoscopy' as source_system
from @catalog_src.@schema_src.src_gireport__lowergi_procedure
