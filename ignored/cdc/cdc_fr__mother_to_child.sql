MODEL (
  name lth_bronze.cdc_fr__mother_to_child,
  kind FULL,
  cron '@daily',
  enabled (1 = 0)
);

WITH cdc AS (
  SELECT
    min(updated_at) AS updated_at
  FROM lth_bronze.cdc__updated_at
  WHERE
    model IN ('fr__mother_to_child')
)
SELECT
  1147314 AS domain_concept_id_1,
  c.person_id AS fact_id_1,
  1147314 AS domain_concept_id_2,
  m.person_id AS fact_id_2,
  40478925 AS relationship_concept_id,
  @generate_surrogate_key(c.person_id, m.person_id) AS unique_key,
  c.last_edit_time
FROM lth_bronze.stg__person AS c
INNER JOIN lth_bronze.stg__person AS m
  ON c.mother_person_source_value = m.person_source_value
WHERE
  c.last_edit_time > (
    SELECT
      dateadd(DAY, -1, updated_at)
    FROM cdc
  )
  AND c.last_edit_time < (
    SELECT
      dateadd(DAY, 365, updated_at)
    FROM cdc
  )
  AND c.last_edit_time <= getdate()
