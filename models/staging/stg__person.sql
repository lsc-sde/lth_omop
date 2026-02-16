MODEL (
  name stg.stg__person,
  kind FULL,
  cron '@daily'
);

WITH person AS (
  SELECT DISTINCT
    mpi.person_id AS person_id,
    mpi.person_source_value AS person_source_value,
    mpi.nhs_number AS nhs_number,
    mpi.source AS source,
    first_value(fp.gender_source_value) OVER (
      PARTITION BY mpi.person_id
      ORDER BY CASE WHEN NOT fp.gender_source_value IS NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC
    ) AS gender_source_value,
    first_value(fp.race_source_value::VARCHAR) OVER (
      PARTITION BY mpi.person_id
      ORDER BY CASE WHEN NOT fp.race_source_value IS NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC
    ) AS race_source_value,
    first_value(fp.mailing_code) OVER (
      PARTITION BY mpi.person_id
      ORDER BY CASE WHEN NOT fp.mailing_code IS NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC
    ) AS mailing_code,
    first_value(fp.provider_id::VARCHAR) OVER (
      PARTITION BY mpi.person_id
      ORDER BY CASE WHEN NOT fp.provider_id::VARCHAR IS NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC
    ) AS provider_id,
    first_value(fp.gp_prac_code) OVER (
      PARTITION BY mpi.person_id
      ORDER BY CASE WHEN NOT fp.gp_prac_code IS NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC
    ) AS gp_prac_code,
    first_value(fp.birth_datetime) OVER (
      PARTITION BY mpi.person_id
      ORDER BY CASE WHEN NOT fp.birth_datetime IS NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC
    ) AS birth_datetime,
    first_value(fp.death_datetime) OVER (
      PARTITION BY mpi.person_id
      ORDER BY CASE WHEN NOT fp.death_datetime IS NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC
    ) AS death_datetime,
    first_value(fp.mother_patient_id) OVER (
      PARTITION BY mpi.person_id
      ORDER BY CASE WHEN NOT fp.mother_patient_id IS NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC
    ) AS mother_person_source_value,
    isnull(
      first_value(fp.last_edit_time) OVER (PARTITION BY mpi.person_id ORDER BY fp.last_edit_time DESC),
      getdate()
    ) AS last_edit_time,
    mpi.source_system AS source_system,
    mpi.org_code AS org_code
  FROM stg.stg__master_patient_index AS mpi
  LEFT JOIN src.src_flex__person AS fp
    ON mpi.flex_patient_id = fp.person_source_value AND mpi.source = 'flex'
  WHERE
    collapsed_into_patient_id IS NULL
)
SELECT
  person.person_id,
  person.person_source_value,
  person.nhs_number,
  person.provider_id,
  person.gp_prac_code,
  person.mother_person_source_value,
  person.birth_datetime,
  person.death_datetime,
  person.gender_source_value,
  person.race_source_value,
  person.mailing_code,
  person.source_system,
  person.org_code,
  person.last_edit_time
FROM person
