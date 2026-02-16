MODEL (
  name vcb.vocab__specimen,
  kind FULL,
  cron '@daily'
);

SELECT
  person_id,
  vc.target_concept_id AS specimen_concept_id,
  32817 AS specimen_type_concept_id,
  order_date AS specimen_date,
  vc.target_concept_name,
  vc_s.target_concept_id AS anatomic_site_concept_id,
  order_number AS specimen_source_id,
  site AS specimen_source_value,
  qualifier AS anatomic_site_source_value,
  measurement_event_id,
  updated_at
FROM stg.stg__specimen AS sp
INNER JOIN (
  SELECT
    target_concept_id AS target_concept_id,
    target_concept_name AS target_concept_name,
    source_code AS source_code
  FROM vcb.vocab__source_to_concept_map
  WHERE
    source_system = 'swisslab' AND concept_group = 'specimen_type'
) AS vc
  ON sp.site = vc.source_code
LEFT JOIN (
  SELECT
    target_concept_id AS target_concept_id,
    source_code AS source_code
  FROM vcb.vocab__source_to_concept_map
  WHERE
    source_system = 'swisslab' AND concept_group = 'anatomical_site'
) AS vc_s
  ON sp.qualifier = vc_s.source_code
