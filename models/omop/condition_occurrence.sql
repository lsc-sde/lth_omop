
MODEL (
  name lth_bronze.condition_occurrence,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

with cons_provider as (
select * from lth_bronze.vocab__provider where cons_org_code is not null
)

with src as (
  select
    abs(cast(cast(
      @generate_surrogate_key(
        person_id,c.provider_id,visit_occurrence_id,condition_concept_id,condition_status_concept_id,condition_start_date,condition_end_date,condition_source_value,c.last_edit_time
        )
     as varbinary(8)) as bigint)) as condition_occurrence_id,
    person_id::bigint as person_id,
    condition_concept_id::bigint as condition_concept_id,
    condition_start_date::date as condition_start_date,
    condition_start_date::datetime as condition_start_datetime,
    condition_end_date::date as condition_end_date,
    condition_end_date::datetime as condition_end_datetime,
    condition_type_concept_id::bigint as condition_type_concept_id,
    condition_status_concept_id::bigint as condition_status_concept_id,
    null::varchar(20) as stop_reason,
    Isnull(pr1.provider_id, pr2.provider_id)::bigint as provider_id,
    visit_occurrence_id::bigint as visit_occurrence_id,
    null::bigint as visit_detail_id,
    condition_source_value::varchar(50) as condition_source_value,
    condition_source_concept_id::bigint as condition_source_concept_id,
    null::varchar(50) as condition_status_source_value,
    @generate_surrogate_key(
      person_id,c.provider_id,visit_occurrence_id,condition_concept_id,condition_status_concept_id,condition_start_date,condition_end_date,condition_source_value,c.last_edit_time
      ) as unique_key,
    c.org_code,
    c.source_system,
    c.last_edit_time,
    c.updated_at,
    getdate() as insert_date_time
  from lth_bronze.vocab__condition_occurrence as c
  left join cons_provider as pr1
    on
      c.provider_id = pr1.cons_org_code
      and c.provider_id_type = 0
  left join lth_bronze.vocab__provider as pr2
    on
      c.provider_id = pr2.provider_source_value
      and c.provider_id_type = 1
), dedup as (
  select *
  from (
    select
      src.*,
      row_number() over (
        partition by unique_key
        order by coalesce(updated_at, last_edit_time) desc
      ) as rn
    from src
  ) t
  where rn = 1
)
select
  condition_occurrence_id,
  person_id,
  condition_concept_id,
  condition_start_date,
  condition_start_datetime,
  condition_end_date,
  condition_end_datetime,
  condition_type_concept_id,
  condition_status_concept_id,
  stop_reason,
  provider_id,
  visit_occurrence_id,
  visit_detail_id,
  condition_source_value,
  condition_source_concept_id,
  condition_status_source_value,
  unique_key,
  org_code,
  source_system,
  last_edit_time,
  updated_at,
  insert_date_time
from dedup