{{
  config(
    materialized = "table",
    tags = ['omop', 'procedure']
    )
}}

select
  cast(row_number() over (order by NEWID()) as bigint)
    as procedure_occurrence_id,
  cast(person_id as bigint) as person_id,
  cast(concept_id as bigint) as procedure_concept_id,
  cast(procedure_date as date) as procedure_date,
  cast(procedure_datetime as datetime) as procedure_datetime,
  cast(null as date) as procedure_end_date,
  cast(null as datetime) as procedure_end_datetime,
  cast(procedure_type_concept_id as bigint) as procedure_type_concept_id,
  cast(null as bigint) as modifier_concept_id,
  cast(1 as int) as quantity,
  cast(provider_id as bigint) as provider_id,
  cast(visit_occurrence_id as bigint) as visit_occurrence_id,
  cast(null as bigint) as visit_detail_id,
  cast(procedure_source_value as varchar(50)) as procedure_source_value,
  cast(null as bigint) as procedure_source_concept_id,
  cast(null as varchar(50)) as modifier_source_value,  
  cast(last_edit_time as datetime) as last_edit_time,
  data_source
from {{ ref('vocab__procedure_occurrence') }} as p
