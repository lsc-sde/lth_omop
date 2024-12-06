{{
  config(
    materialized = "table"
    )
}}

select
  cast(null as bigint) as note_nlp_id,
  cast(null as bigint) as note_id,
  cast(null as bigint) as section_concept_id,
  cast(null as varchar(250)) as snippet,
  cast(null as varchar(50)) as offset,
  cast(null as varchar(250)) as lexical_variant,
  cast(null as bigint) as note_nlp_concept_id,
  cast(null as bigint) as note_nlp_source_concept_id,
  cast(null as varchar(250)) as nlp_system,
  cast(null as date) as nlp_date,
  cast(null as datetime) as nlp_datetime,
  cast(null as varchar(1)) as term_exists,
  cast(null as varchar(50)) as term_temporal,
  cast(null as varchar(2000)) as term_modifiers
