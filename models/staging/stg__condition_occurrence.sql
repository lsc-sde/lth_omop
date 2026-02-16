MODEL (
  name stg.stg__condition_occurrence,
  kind FULL,
  cron '@daily'
);

WITH cond AS (
  SELECT
    mpi.person_id AS person_id,
    fco.visit_occurrence_id AS visit_occurrence_id,
    fco.episode_start_dt AS episode_start_dt,
    fco.episode_end_dt AS episode_end_dt,
    fco.provider_id AS provider_id,
    fco.source_code AS source_code,
    fco.index_nbr AS episode_coding_position,
    fco.org_code AS org_code,
    fco.source_system AS source_system,
    fco.last_edit_time AS last_edit_time,
    fco.updated_at AS updated_at
  FROM stg.stg_flex__condition_occurrence AS fco
  INNER JOIN stg.stg__master_patient_index AS mpi
    ON fco.patient_id = mpi.flex_patient_id
), person AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY person_id ORDER BY last_edit_time DESC) AS id
  FROM stg.stg__master_patient_index
)
SELECT
  person_id,
  visit_occurrence_id,
  episode_start_dt,
  episode_end_dt,
  provider_id,
  source_code,
  row_number() OVER (
    PARTITION BY visit_occurrence_id
    ORDER BY episode_end_dt::DATE DESC, episode_start_dt::DATE DESC, isnull(episode_coding_position, 99) ASC
  ) AS episode_coding_position,
  org_code,
  source_system,
  last_edit_time,
  updated_at
FROM cond
