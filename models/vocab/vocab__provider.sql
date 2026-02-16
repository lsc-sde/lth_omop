MODEL (
  name vcb.vocab__provider,
  kind FULL,
  cron '@daily'
);

SELECT
  provider_name,
  care_site_id,
  provider_source_value,
  provider_id,
  specialty_source_value,
  vm.source_code_description,
  vm.target_concept_id,
  cons_org_code,
  source_system,
  org_code
FROM stg.stg__provider AS p
LEFT JOIN (
  SELECT DISTINCT
    source_code_description AS source_code_description,
    target_concept_id AS target_concept_id
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'specialty'
) AS vm
  ON p.specialty_source_value = vm.source_code_description
