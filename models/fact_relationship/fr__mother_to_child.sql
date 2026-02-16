MODEL (
  name stg.fr__mother_to_child,
  cron '@daily',
  kind FULL,
  enabled (1 = 1)
);

SELECT
  1147314 AS domain_concept_id_1,
  c.person_id AS fact_id_1,
  1147314 AS domain_concept_id_2,
  m.person_id AS fact_id_2,
  40478925 AS relationship_concept_id,
  @generate_surrogate_key(c.person_id, m.person_id) AS unique_key,
  c.last_edit_time
FROM stg.stg__person AS c
INNER JOIN stg.stg__person AS m
  ON c.mother_person_source_value = m.person_source_value
