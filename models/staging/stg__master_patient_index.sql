MODEL (
  name lth_bronze.stg__master_patient_index,
  kind FULL,
  cron '@daily'
);

WITH flex_person AS (
  SELECT
    person_source_value AS flex_patient_id,
    mrn AS flex_mrn,
    nhs_number::NUMERIC(10, 0) AS nhs_number,
    birth_datetime AS birth_datetime,
    death_datetime AS death_datetime,
    last_edit_time AS last_edit_time,
    source_system AS source_system,
    org_code AS org_code
  FROM lth_bronze.src_flex__person
  GROUP BY
    person_source_value,
    mrn,
    birth_datetime,
    death_datetime,
    nhs_number,
    last_edit_time,
    source_system,
    org_code
), sl_person AS (
  SELECT
  TOP 1
    NULL AS sl_patient_id,
    NULL AS flex_mrn,
    NULL AS nhs_number,
    NULL AS data_source,
    NULL AS birth_datetime,
    NULL AS death_datetime,
    NULL AS last_edit_time,
    NULL AS source_system,
    NULL AS org_code
), nhs_numbers AS (
  SELECT
    nhs_number
  FROM flex_person
  WHERE
    NOT nhs_number IS NULL
  UNION
  SELECT DISTINCT
    nhs_number
  FROM sl_person
  WHERE
    NOT nhs_number IS NULL
), mpi_base AS (
  SELECT DISTINCT
    nn.nhs_number AS nhs_number,
    fp.flex_patient_id AS flex_patient_id,
    fp.flex_mrn AS flex_mrn,
    NULL AS scr_patient_id,
    fp.birth_datetime AS birth_datetime,
    fp.death_datetime AS death_datetime,
    fp.last_edit_time AS last_edit_time,
    fp.source_system AS source_system,
    fp.org_code AS org_code
  FROM nhs_numbers AS nn
  LEFT JOIN flex_person AS fp
    ON nn.nhs_number = fp.nhs_number
), mpi_base_flex AS (
  SELECT
    *
  FROM mpi_base
  UNION ALL
  SELECT
    nhs_number,
    fp.flex_patient_id,
    fp.flex_mrn,
    NULL AS scr_patient_id,
    fp.birth_datetime,
    death_datetime,
    last_edit_time,
    source_system,
    org_code
  FROM flex_person AS fp
  WHERE
    NOT flex_patient_id IN (
      SELECT DISTINCT
        flex_patient_id
      FROM mpi_base
      WHERE
        NOT flex_patient_id IS NULL
    )
), mpi_base_sl AS (
  SELECT
    *
  FROM mpi_base_flex
  UNION ALL
  SELECT
    nhs_number,
    NULL AS flex_patient_id,
    NULL AS flex_mrn,
    NULL AS scr_patient_id,
    birth_datetime,
    death_datetime,
    last_edit_time,
    source_system,
    org_code
  FROM sl_person
), mpi_final_base AS (
  SELECT
    mpi.*,
    CASE
      WHEN NOT mpi.flex_mrn IS NULL OR NOT mpi.flex_patient_id IS NULL
      THEN 'flex'
      ELSE 'sl'
    END AS source,
    count(mpi.nhs_number) OVER (PARTITION BY mpi.nhs_number) AS nhs_number_count,
    count(mpi.flex_patient_id) OVER (PARTITION BY mpi.flex_patient_id) AS flex_patient_id_count,
    count(mpi.flex_mrn) OVER (PARTITION BY mpi.flex_mrn) AS flex_mrn_count /* , */
  FROM mpi_base_sl AS mpi
  WHERE
    NOT mpi.flex_patient_id IS NULL
), mpi_final_all AS (
  SELECT
    translate(
      coalesce(p.collapsed_into_patient_id, mpi.flex_patient_id)::VARCHAR,
      '1234567890',
      '9876543210'
    )::BIGINT AS person_id,
    coalesce(p.collapsed_into_patient_id, mpi.flex_patient_id) AS person_source_value,
    mpi.*
  FROM mpi_final_base AS mpi
  LEFT JOIN lth_bronze.src_flex__person AS p
    ON mpi.flex_patient_id = p.person_source_value
), collapsed_nhs AS (
  SELECT DISTINCT
    nhs_number AS nhs_number,
    person_source_value AS person_source_value,
    person_id AS person_id,
    row_number() OVER (PARTITION BY nhs_number ORDER BY person_source_value) AS id
  FROM mpi_final_all
  WHERE
    person_source_value = flex_patient_id AND NOT nhs_number IS NULL
)
SELECT
  coalesce(cn.person_id, mpi.person_id) AS person_id,
  coalesce(cn.person_source_value, mpi.person_source_value) AS person_source_value,
  coalesce(cn.nhs_number, mpi.nhs_number) AS nhs_number,
  flex_patient_id,
  flex_mrn,
  scr_patient_id,
  birth_datetime,
  death_datetime,
  last_edit_time,
  source,
  nhs_number_count,
  flex_patient_id_count,
  flex_mrn_count,
  NULL AS scr_patient_id_count,
  row_number() OVER (PARTITION BY coalesce(cn.person_id, mpi.person_id) ORDER BY last_edit_time DESC) AS current_record,
  @generate_surrogate_key(
    flex_patient_id,
    flex_mrn,
    coalesce(scr_patient_id, 0),
    coalesce(cn.nhs_number, mpi.nhs_number)
  ) AS unique_key,
  source_system,
  org_code
FROM mpi_final_all AS mpi
LEFT JOIN collapsed_nhs AS cn
  ON mpi.nhs_number = cn.nhs_number AND id = 1
