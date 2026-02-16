MODEL (
  name vcb.vocab__drug_exposure,
  kind FULL,
  cron '@daily'
);

WITH drugs AS (
  SELECT
    mpi.person_id AS person_id,
    fdo.visit_occurrence_id AS visit_occurrence_id,
    fdo.drug_exposure_start_datetime AS drug_exposure_start_datetime,
    fdo.provider_id AS provider_id,
    fdo.flex_procedure_id AS flex_procedure_id,
    fdo.drug_source_value AS drug_source_value,
    fdo.dosage AS dosage,
    fdo.adm_route AS adm_route,
    fdo.org_code AS org_code,
    fdo.source_system AS source_system,
    fdo.last_edit_time AS last_edit_time,
    fdo.updated_at AS updated_at
  FROM stg.stg_flex__drug_exposure AS fdo
  INNER JOIN stg.stg__master_patient_index AS mpi
    ON fdo.person_source_value = mpi.flex_patient_id
)
SELECT DISTINCT
  visit_occurrence_id,
  person_id,
  c_cm.target_concept_id,
  drug_exposure_start_datetime,
  provider_id,
  32838 AS drug_type_concept_id,
  r_cm.target_concept_id AS route_concept_id,
  drug_source_value,
  dosage,
  adm_route,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  last_edit_time,
  updated_at
FROM drugs AS de
INNER JOIN (
  SELECT DISTINCT
    source_code AS source_code,
    target_concept_id AS target_concept_id
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'drugs'
) AS c_cm
  ON de.flex_procedure_id::VARCHAR = c_cm.source_code
INNER JOIN (
  SELECT
    target_concept_id AS target_concept_id,
    source_code AS source_code
  FROM vcb.vocab__source_to_concept_map
  WHERE
    concept_group = 'drug_routes'
) AS r_cm
  ON de.adm_route::VARCHAR = r_cm.source_code
