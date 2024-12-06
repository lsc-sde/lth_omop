{{
  config(
    materialized = "table"
    )
}}

select
  cast(null as bigint) as episode_id,
  cast(null as bigint) as person_id,
  cast(null as bigint) as episode_concept_id,
  cast(null as date) as episode_start_date,
  cast(null as datetime) as episode_start_datetime,
  cast(null as date) as episode_end_date,
  cast(null as datetime) as episode_end_datetime,
  cast(null as bigint) as episode_parent_id,
  cast(null as int) as episode_number,
  cast(null as bigint) as episode_object_concept_id,
  cast(null as bigint) as episode_type_concept_id,
  cast(null as varchar(50)) as episode_source_value,
  cast(null as bigint) as episode_source_concept_id
