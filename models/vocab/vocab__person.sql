MODEL (
  name vcb.vocab__person,
  kind VIEW,
  cron '@daily'
);

SELECT
  person_id,
  provider_id,
  gp_prac_code,
  birth_datetime,
  0 AS ethnicity_concept_id, /* unknown ethnicity code */
  mailing_code,
  person_source_value,
  gender_source_value,
  race_source_value,
  CASE
    WHEN gender_source_value LIKE '%Unknown%'
    THEN '8551'
    ELSE v.target_concept_id
  END AS gender_concept_id,
  isnull(
    CASE
      WHEN v2.target_concept_id IN (46273465, 35607960)
      THEN 0
      ELSE v2.target_concept_id
    END,
    0
  ) AS race_concept_id,
  source_system,
  org_code,
  last_edit_time
FROM stg.stg__person AS p
LEFT JOIN (
  SELECT
    target_concept_id AS target_concept_id,
    source_code AS source_code
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'demographics'
) AS v
  ON p.gender_source_value = v.source_code
LEFT JOIN (
  SELECT
    target_concept_id AS target_concept_id,
    source_code AS source_code,
    source_code_description AS source_code_description
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'demographics'
) AS v2
  ON p.race_source_value::VARCHAR = v2.source_code_description
