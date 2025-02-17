
MODEL (
  name lth_bronze.procedure_occurrence,
  kind FULL,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

select
  person_id::bigint as person_id,
  concept_id::bigint as procedure_concept_id,
  procedure_date::date as procedure_date,
  procedure_datetime::datetime as procedure_datetime,
  null::date as procedure_end_date,
  null::datetime as procedure_end_datetime,
  procedure_type_concept_id::bigint as procedure_type_concept_id,
  null::bigint as modifier_concept_id,
  1::int as quantity,
  provider_id::bigint as provider_id,
  visit_occurrence_id::bigint as visit_occurrence_id,
  null::bigint as visit_detail_id,
  procedure_source_value::varchar(50) as procedure_source_value,
  null::bigint as procedure_source_concept_id,
  null::varchar(50) as modifier_source_value, 
  @generate_surrogate_key(person_id, visit_occurrence_id, concept_id, procedure_datetime, last_edit_time) as unique_key,
  org_code::varchar(5),
  source_system::varchar(20),
  last_edit_time::datetime,
  getdate()::datetime as insert_date_time
from lth_bronze.vocab__procedure_occurrence as p