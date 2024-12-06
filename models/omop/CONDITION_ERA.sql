{{
  config(
    materialized = "table",
    tags = ['omop', 'condition', 'era']
    )
}}

SELECT
  CAST(ROW_NUMBER() OVER (
    ORDER BY person_id
  ) AS bigint) AS condition_era_id,
  CAST(person_id AS bigint) AS person_id,
  CAST(CONDITION_CONCEPT_ID AS bigint) AS condition_concept_id,
  min(CONDITION_START_DATE) AS CONDITION_ERA_START_DATE,
  ERA_END_DATE AS CONDITION_ERA_END_DATE,
  count(*) AS CONDITION_OCCURRENCE_COUNT
FROM {{ ref('era__condition_ends') }}
GROUP BY
  person_id,
  CONDITION_CONCEPT_ID,
  ERA_END_DATE
