
MODEL (
  name lth_bronze.src_gireport__uppergi_procedure,
  kind VIEW,
  cron '@daily',
);

select
person_source_value,
visit_occurrence_id,
care_site_id,
procedure_date,
procedure_datetime,
procedure_concept_id,
procedure_source_value,
provider_id
from @catalog_src.@schema_src.src_gireport__uppergi_procedure
