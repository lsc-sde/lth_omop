
MODEL (
  name src.src_gireport__lowergi_therapeutic,
  kind VIEW,
  cron '@daily',
);

select
  person_source_value,
  visit_occurrence_id,
  anatomic_site_source_value,
  parent_procedure_source_value,
  procedure_source_value,
  'rxn' as org_code,
  'endoscopy' as source_system
from @catalog_src.@schema_src.src_gireport__lowergi_therapeutic
