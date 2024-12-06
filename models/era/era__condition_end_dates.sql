{{
  config(
    materialized = "table",
    tags = ['omop', 'era', 'condition']
    )
}}

SELECT
  PERSON_ID,
  CONDITION_CONCEPT_ID,
  DATEADD(DAY, -30, EVENT_DATE) AS END_DATE -- unpad the end date
FROM (
  SELECT
    E1.PERSON_ID,
    E1.CONDITION_CONCEPT_ID,
    E1.EVENT_DATE,
    COALESCE(E1.START_ORDINAL, MAX(E2.START_ORDINAL)) AS START_ORDINAL,
    E1.OVERALL_ORD
  FROM (
    SELECT
      PERSON_ID,
      CONDITION_CONCEPT_ID,
      EVENT_DATE,
      EVENT_TYPE,
      START_ORDINAL,
      ROW_NUMBER() OVER (
        PARTITION BY PERSON_ID,
        CONDITION_CONCEPT_ID ORDER BY
          EVENT_DATE,
          EVENT_TYPE
      ) AS OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
    FROM (
      -- select the start dates, assigning a row number to each
      SELECT
        PERSON_ID,
        CONDITION_CONCEPT_ID,
        CONDITION_START_DATE AS EVENT_DATE,
        -1 AS EVENT_TYPE,
        ROW_NUMBER() OVER (
          PARTITION BY PERSON_ID,
          CONDITION_CONCEPT_ID ORDER BY CONDITION_START_DATE
        ) AS START_ORDINAL
      FROM lth_bronze.era__condition_target 

      UNION ALL

      -- pad the end dates by 30 to allow a grace period for overlapping ranges.
      SELECT
        PERSON_ID,
        CONDITION_CONCEPT_ID,
        DATEADD(DAY, 30, CONDITION_END_DATE),
        1 AS EVENT_TYPE,
        NULL
      FROM lth_bronze.era__condition_target 
    ) AS RAWDATA
  ) AS E1
  INNER JOIN (
    SELECT
      PERSON_ID,
      CONDITION_CONCEPT_ID,
      CONDITION_START_DATE AS EVENT_DATE,
      ROW_NUMBER() OVER (
        PARTITION BY PERSON_ID,
        CONDITION_CONCEPT_ID ORDER BY CONDITION_START_DATE
      ) AS START_ORDINAL
    FROM lth_bronze.era__condition_target 
  ) AS E2
    ON
      E1.PERSON_ID = E2.PERSON_ID
      AND E1.CONDITION_CONCEPT_ID = E2.CONDITION_CONCEPT_ID
      AND E2.EVENT_DATE <= E1.EVENT_DATE
  GROUP BY
    E1.PERSON_ID,
    E1.CONDITION_CONCEPT_ID,
    E1.EVENT_DATE,
    E1.START_ORDINAL,
    E1.OVERALL_ORD
) AS E
WHERE (2 * E.START_ORDINAL) - E.OVERALL_ORD = 0
