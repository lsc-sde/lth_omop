
MODEL (
  name lth_bronze.procedure_occurrence,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

with src as (
  select
    abs(cast(cast(
      @generate_surrogate_key(
        person_id, visit_occurrence_id, concept_id, procedure_datetime, last_edit_time
        )
    as varbinary(8)) as bigint)) as procedure_occurrence_id,
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
    @generate_surrogate_key(
      person_id, visit_occurrence_id, concept_id, procedure_datetime, last_edit_time
      ) as unique_key,
    org_code::varchar(5),
    source_system::varchar(20),
    last_edit_time::datetime,
    getdate()::datetime as insert_date_time
  from lth_bronze.vocab__procedure_occurrence as p
), dedup as (
  select *
  from (
    select
      src.*,
      row_number() over (
        partition by unique_key
        order by last_edit_time desc
      ) as rn
    from src
  ) t
  where rn = 1
)
select
  procedure_occurrence_id,
  person_id,
  procedure_concept_id,
  procedure_date,
  procedure_datetime,
  procedure_end_date,
  procedure_end_datetime,
  procedure_type_concept_id,
  modifier_concept_id,
  quantity,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  procedure_source_value,
  procedure_source_concept_id,
  modifier_source_value,
  unique_key,
  org_code,
  source_system,
  last_edit_time,
  insert_date_time
from dedup