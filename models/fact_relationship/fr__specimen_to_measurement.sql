MODEL (
  name lth_bronze.fr__specimen_to_measurement,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  ),
  enabled (0 = 1)
);

SELECT
  s.domain_concept_id_1,
  s.fact_id_1,
  s.domain_concept_id_2,
  s.fact_id_2,
  s.relationship_concept_id,
  s.specimen_event_id,
  s.measurement_key,
  s.unique_key,
  s.last_edit_time
FROM lth_bronze.cdc_fr__specimen_to_measurement AS s
