MODEL (
  name lth_bronze.cdc_fr__specimen_to_measurement,
  kind FULL,
  cron '@daily',
  enabled (1 = 0)
);

WITH cdc AS (
  SELECT
    min(updated_at) AS updated_at
  FROM lth_bronze.cdc__updated_at
  WHERE
    model IN ('fr__specimen_to_measurement')
), specimen AS (
  SELECT
    *
  FROM lth_bronze.specimen AS s
  WHERE
    s.updated_at > (
      SELECT
        dateadd(DAY, -5, updated_at)
      FROM cdc
    )
), measurement AS (
  SELECT
    *
  FROM lth_bronze.measurement AS s
  WHERE
    measurement_event_id IN (
      SELECT
        specimen_event_id
      FROM specimen
    )
)
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
FROM specimen AS s
INNER JOIN measurement AS ms
  ON s.specimen_event_id = ms.measurement_event_id
WHERE
  s.updated_at > (
    SELECT
      dateadd(DAY, -5, updated_at)
    FROM cdc
  )
  AND s.updated_at < (
    SELECT
      dateadd(DAY, 365, updated_at)
    FROM cdc
  )
  AND s.updated_at <= getdate()
