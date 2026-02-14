MODEL (
  name lth_bronze.fact_relationship,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  ),
  enabled (0 = 1)
);

SELECT
  domain_concept_id_1::BIGINT,
  fact_id_1::BIGINT,
  domain_concept_id_2::BIGINT,
  fact_id_2::BIGINT,
  relationship_concept_id::BIGINT,
  unique_key,
  last_edit_time::DATETIME
FROM lth_bronze.fr__observation_to_measurement_sl
UNION ALL
SELECT
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  unique_key,
  last_edit_time::DATETIME
FROM lth_bronze.fr__measurement_to_observation_sl
UNION ALL
SELECT
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  unique_key,
  last_edit_time::DATETIME
FROM lth_bronze.fr__specimen_to_measurement
UNION ALL
SELECT
  domain_concept_id_1,
  fact_id_1,
  domain_concept_id_2,
  fact_id_2,
  relationship_concept_id,
  unique_key,
  last_edit_time::DATETIME
FROM lth_bronze.fr__mother_to_child
