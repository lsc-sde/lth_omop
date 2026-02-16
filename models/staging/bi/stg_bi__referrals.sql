MODEL (
  name stg.stg_bi__referrals,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column last_edit_time,
    batch_size 30,
    batch_concurrency 4
  ),
  cron '@daily'
);

SELECT
  bi.patient_id AS bi_patient_id,
  bi.visit_id AS visit_occurrence_id,
  NULL AS measurement_event_id,
  bi.referral_received_date::SMALLDATETIME AS referral_received_date,
  pr.provider_id,
  bi.treatment_function_code AS source_code,
  bi.treatment_function_name AS source_name,
  NULL AS value_source_value,
  NULL AS source_value,
  NULL AS value_as_number,
  NULL AS unit_source_value,
  coalesce(bi.suspected_cancer_type, bi.consultant_priority, bi.gp_priority) AS priority,
  bi.source_system::VARCHAR(20),
  bi.org_code::VARCHAR(5),
  bi.last_edit_time::DATETIME2 AS last_edit_time,
  bi.updated_at::DATETIME2 AS updated_at
FROM src.src_bi__referrals AS bi
LEFT JOIN stg.stg__provider AS pr
  ON bi.referring_emp_code = pr.provider_source_value
WHERE
  bi.last_edit_time BETWEEN @start_ds AND @end_ds
