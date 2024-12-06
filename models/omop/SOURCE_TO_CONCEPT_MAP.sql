{{
  config(
    materialized = "table",
    tags = ['omop', 'visit_occurrence', 'visit']
    )
}}

select
  cast(source_code as varchar(50)) as source_code,
  cast(0 as bigint) as source_concept_id,
  cast(44819096 as varchar(20)) as source_vocabulary_id,
  cast(source_code_description as varchar(255)) as source_code_description,
  cast(target_concept_id as bigint) as target_concept_id,
  cast(c.vocabulary_id as varchar(20)) as target_vocabulary_id,
  c.valid_start_date,
  c.valid_end_date,
  c.invalid_reason,
  frequency
from lth_bronze.vocab__source_to_concept_map_raw as r
left join vocab.CONCEPT as c
  on r.target_concept_id = c.concept_id
where
  mapping_status = 'approved'
  and source_code is not null
