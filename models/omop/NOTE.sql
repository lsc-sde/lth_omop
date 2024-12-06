{{
  config(
    materialized = "table"
    )
}}

select
  cast(null as bigint) as note_id,
  cast(null as bigint) as person_id,
  cast(null as date) as note_date,
  cast(null as datetime) as note_datetime,
  cast(null as bigint) as note_type_concept_id,
  cast(null as bigint) as note_class_concept_id,
  cast(null as varchar(250)) as note_title,
  cast(null as varchar(8000)) as note_text,
  cast(null as bigint) as encoding_concept_id,
  cast(null as bigint) as language_concept_id,
  cast(null as bigint) as provider_id,
  cast(null as bigint) as visit_occurrence_id,
  cast(null as bigint) as visit_detail_id,
  cast(null as varchar(50)) as note_source_value,
  cast(null as bigint) as note_event_id,
  cast(null as bigint) as note_event_field_concept_id
