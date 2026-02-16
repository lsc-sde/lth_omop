MODEL (
  name stg.fr__measurement_to_observation_sl,
  cron '@daily',
  kind FULL,
  enabled (
    0 = 1
  )
);

WITH measures AS (
  SELECT
    ms.measurement_id AS measurement_id,
    ms.person_id AS person_id,
    ms.measurement_event_id AS measurement_event_id,
    ms.measurement_datetime AS measurement_datetime,
    ms.last_edit_time AS last_edit_time,
    ms.unique_key AS unique_key
  FROM cdm.measurement AS ms
  INNER JOIN (
    SELECT DISTINCT
      source_code AS source_code,
      target_concept_id AS target_concept_id,
      target_domain_id AS target_domain_id,
      concept_group AS concept_group
    FROM vcb.vocab__source_to_concept_map
    WHERE
      (
        concept_group = 'bacteria_presence' OR concept_group = 'bacteriology_other_test'
      )
  ) AS m
    ON ms.measurement_source_value = m.source_code
  LEFT JOIN stg.stg_sl__bacteriology AS slb
    ON ms.measurement_event_id = slb.measurement_event_id
  WHERE
    slb.source_system = 'swl' AND slb.updated_at <= getdate()
), observes AS (
  SELECT
    person_id AS person_id,
    observation_id AS observation_id,
    observation_event_id AS observation_event_id,
    unique_key AS unique_key
  FROM cdm.observation
  WHERE
    person_id IN (
      SELECT DISTINCT
        person_id
      FROM measures
    )
)
SELECT DISTINCT
  1147330 AS domain_concept_id_1,
  ms.measurement_id AS fact_id_1,
  1147304 AS domain_concept_id_2,
  ob.observation_id AS fact_id_2,
  581411 AS relationship_concept_id,
  ob.unique_key AS observation_key,
  ms.unique_key AS measurement_key,
  @generate_surrogate_key(ms.measurement_id, ob.observation_id, ms.last_edit_time) AS unique_key,
  ms.last_edit_time
FROM measures AS ms
INNER JOIN stg.stg_sl__bacteriology AS slb
  ON ms.measurement_event_id = slb.measurement_event_id
INNER JOIN observes AS ob
  ON slb.measurement_event_id = ob.observation_event_id
  AND ob.person_id = ms.person_id
