MODEL (
  name stg.stg_flex__ae_diagnosis,
  kind FULL,
  cron '@daily'
);

SELECT
  visit_id,
  patient_id,
  126 AS provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  org_code::VARCHAR(5),
  source_system::VARCHAR(20),
  last_edit_time,
  updated_at,
  value AS diagnosis_list
FROM (
  SELECT
    visit_id AS visit_id,
    patient_id AS patient_id,
    activation_time AS activation_time,
    admission_date_time AS admission_date_time,
    discharge_date_time AS discharge_date_time,
    org_code::VARCHAR(5) AS org_code,
    last_edit_time AS last_edit_time,
    updated_at AS updated_at,
    value AS diag_list,
    'flex_ae' AS source_system
  FROM src.src_flex__ae_diagnosis CROSS APPLY string_split(diag_list, '~')
) AS t CROSS APPLY string_split(diag_list, '|')
WHERE
  len(value) > 1 AND NOT value IN ('410605003')
