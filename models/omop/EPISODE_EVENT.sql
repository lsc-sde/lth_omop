{{
  config(
    materialized = "table"
    )
}}

select
  cast(null as bigint) as episode_id,
  cast(null as bigint) as event_id,
  cast(null as bigint) as episode_event_field_concept_id
