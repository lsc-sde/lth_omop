MODEL (
  name lth_bronze.fr__specimen_to_measurement,
  cron '@daily',
  kind FULL,
  enabled (0 = 1)
);

SELECT DISTINCT
  1147306 AS domain_concept_id_1,
  specimen_id AS fact_id_1,
  1147330 AS domain_concept_id_2,
  ms.measurement_id AS fact_id_2,
  32669 AS relationship_concept_id,
  specimen_event_id,
  ms.unique_key AS measurement_key,
  @generate_surrogate_key(specimen_id, ms.measurement_id, s.updated_at) AS unique_key,
  s.updated_at AS last_edit_time
FROM cdm.specimen AS s
INNER JOIN cdm.measurement AS ms
  ON s.specimen_event_id = ms.measurement_event_id
WHERE
  s.updated_at <= getdate()
