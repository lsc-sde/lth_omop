{{
  config(
    materialized = "table",
    tags = ['omop', 'era', 'condition']
    )
}}

select
  co.PERSON_ID,
  co.condition_concept_id,
  co.CONDITION_START_DATE,
  coalesce(co.CONDITION_END_DATE, DATEADD(day, 1, co.CONDITION_START_DATE))
    as CONDITION_END_DATE
from {{ ref('CONDITION_OCCURRENCE') }} as co
