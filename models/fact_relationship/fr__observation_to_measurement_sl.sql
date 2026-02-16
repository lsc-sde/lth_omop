MODEL (
  name stg.fr__observation_to_measurement_sl,
  cron '@daily',
  kind FULL,
  enabled (0 = 1)
);

WITH  observes AS (
  SELECT
    ob.observation_id AS observation_id,
    ob.person_id AS person_id,
    ob.observation_event_id AS observation_event_id,
    ob.observation_datetime AS observation_datetime,
    slb.isolate_event_id AS isolate_event_id,
    ob.unique_key AS unique_key,
    ob.last_edit_time AS last_edit_time
  FROM cdm.observation AS ob
  INNER JOIN (
    SELECT DISTINCT
      source_code AS source_code,
      target_concept_id AS target_concept_id,
      target_domain_id AS target_domain_id,
      concept_group AS concept_group
    FROM vcb.vocab__source_to_concept_map
    WHERE
      concept_group = 'bacteria_observation'
  ) AS m
    ON ob.observation_source_value = m.source_code
  LEFT JOIN stg.stg_sl__bacteriology AS slb
    ON ob.observation_event_id = slb.measurement_event_id
  WHERE slb.updated_at <= getdate()
    AND slb.source_system = 'swl'
), measures AS (
  SELECT
    person_id AS person_id,
    measurement_id AS measurement_id,
    measurement_event_id AS measurement_event_id,
    unique_key AS unique_key
  FROM cdm.measurement
  WHERE
    person_id IN (
      SELECT DISTINCT
        person_id
      FROM observes
    )
)
SELECT DISTINCT
  1147304 AS domain_concept_id_1,
  ob.observation_id AS fact_id_1,
  1147330 AS domain_concept_id_2,
  m.measurement_id AS fact_id_2,
  581410 AS relationship_concept_id,
  ob.unique_key AS observation_key,
  m.unique_key AS measurement_key,
  @generate_surrogate_key(ob.observation_id, m.measurement_id, ob.last_edit_time) AS unique_key,
  ob.last_edit_time
FROM observes AS ob
INNER JOIN stg.stg_sl__bacteriology AS slb
  ON ob.isolate_event_id = slb.isolate_event_id
  AND ob.observation_event_id <> slb.measurement_event_id
INNER JOIN measures AS m
  ON slb.measurement_event_id = m.measurement_event_id AND ob.person_id = m.person_id
