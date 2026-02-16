MODEL (
  name cdm.drug_exposure,
  kind FULL,
  cron '@daily'
);

SELECT
  @generate_surrogate_key(
    person_id,
    visit_occurrence_id,
    de.target_concept_id,
    dosage,
    drug_exposure_start_datetime,
    adm_route,
    last_edit_time
  )::VARBINARY(16) AS drug_exposure_id,
  de.person_id::BIGINT AS person_id,
  de.target_concept_id::BIGINT AS drug_concept_id,
  drug_exposure_start_datetime::DATE AS drug_exposure_start_date,
  drug_exposure_start_datetime::DATETIME AS drug_exposure_start_datetime,
  drug_exposure_start_datetime::DATE AS drug_exposure_end_date,
  drug_exposure_start_datetime::DATETIME AS drug_exposure_end_datetime,
  NULL::DATE AS verbatim_end_date,
  drug_type_concept_id::BIGINT AS drug_type_concept_id,
  NULL::VARCHAR(20) AS stop_reason,
  NULL::INTEGER AS refills,
  NULL::FLOAT AS quantity,
  NULL::INTEGER AS days_supply,
  NULL::VARCHAR(8000) AS sig,
  route_concept_id::BIGINT AS route_concept_id,
  NULL::VARCHAR(50) AS lot_number,
  pr.provider_id::BIGINT AS provider_id,
  visit_occurrence_id::BIGINT AS visit_occurrence_id,
  NULL::BIGINT AS visit_detail_id,
  drug_source_value::VARCHAR(50) AS drug_source_value,
  NULL::BIGINT AS drug_source_concept_id,
  adm_route::VARCHAR(50) AS route_source_value,
  NULL::VARCHAR(50) AS dose_unit_source_value,
  de.org_code::VARCHAR(5),
  de.source_system::VARCHAR(20),
  de.last_edit_time::DATETIME,
  getdate()::DATETIME AS insert_date_time
FROM vcb.vocab__drug_exposure AS de
LEFT JOIN cdm.provider AS pr
  ON de.provider_id = pr.provider_source_value
