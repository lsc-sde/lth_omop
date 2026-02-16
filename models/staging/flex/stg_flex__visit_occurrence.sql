MODEL (
  name stg.stg_flex__visit_occurrence,
  kind FULL,
  cron '@daily'
);

SELECT DISTINCT
  vs.person_source_value,
  vs.visit_number,
  coalesce(earliest_activation_time, activation_time) AS activation_time,
  coalesce(ft.visit_type_id, vs.visit_type_id) AS visit_type_id,
  CASE WHEN NOT ft.visit_id IS NULL THEN ft.first_visit_id ELSE vs.visit_id END AS visit_id,
  coalesce(ft.latest_status, vs.visit_status_id) AS visit_status_id,
  CASE
    WHEN NOT ft.visit_id IS NULL
    THEN earliest_admission_time
    ELSE admission_date_time
  END AS admission_time,
  CASE
    WHEN NOT ft.visit_id IS NULL
    THEN latest_discharge_time
    ELSE discharge_date_time
  END AS discharge_time,
  CASE
    WHEN NOT ft.visit_id IS NULL
    THEN first_attending_emp_provider_id
    ELSE attending_emp_provider_id
  END AS provider_id,
  CASE WHEN NOT ft.visit_id IS NULL THEN first_facility ELSE facility_id END AS facility_id,
  CASE
    WHEN NOT ft.visit_id IS NULL
    THEN first_admission_source
    ELSE admission_source
  END::INTEGER AS admitted_from_source_value,
  CASE
    WHEN NOT ft.visit_id IS NULL
    THEN last_discharge_dest
    ELSE discharge_dest_code
  END::INTEGER AS discharged_to_source_value,
  vs.source_system::VARCHAR(20),
  vs.org_code::VARCHAR(5),
  coalesce(ft.last_edit_time, vs.last_edit_time) AS last_edit_time,
  coalesce(ft.updated_at, vs.updated_at) AS updated_at
FROM src.src_flex__visit_segment AS vs
LEFT JOIN stg.stg_flex__facility_transfer AS ft
  ON vs.visit_number = ft.visit_number
