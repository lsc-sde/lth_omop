MODEL (
  name stg.stg_flex__facility_transfer,
  kind FULL,
  cron '@daily'
);

WITH multiple_visits AS (
  SELECT DISTINCT
    visit_number AS visit_number,
    visit_id AS visit_id,
    count(*) OVER (PARTITION BY visit_number) AS total_records
  FROM src.src_flex__visit_segment
  GROUP BY
    visit_number,
    visit_id
), merged_visits AS (
  SELECT
    vs.visit_id AS visit_id,
    vs.visit_number AS visit_number,
    person_source_value AS person_source_value,
    first_value(visit_type_id) OVER (PARTITION BY vs.visit_number ORDER BY activation_time) AS visit_type_id,
    count(*) OVER (PARTITION BY vs.visit_number) AS total_activations,
    count(*) OVER (PARTITION BY vs.visit_id) AS total_entries,
    row_number() OVER (PARTITION BY vs.visit_number, vs.visit_id ORDER BY activation_time) AS visit_activation_sequence_number,
    row_number() OVER (PARTITION BY vs.visit_number ORDER BY activation_time) AS activation_sequence_number,
    first_value(vs.visit_id) OVER (PARTITION BY vs.visit_number ORDER BY activation_time) AS first_visit_id,
    first_value(attending_emp_provider_id) OVER (PARTITION BY vs.visit_number ORDER BY activation_time) AS first_attending_emp_provider_id,
    first_value(facility_id) OVER (PARTITION BY vs.visit_number ORDER BY activation_time) AS first_facility,
    first_value(visit_status_id) OVER (PARTITION BY vs.visit_number ORDER BY activation_time DESC) AS latest_status,
    first_value(admission_source) OVER (PARTITION BY vs.visit_number ORDER BY activation_time) AS first_admission_source,
    first_value(discharge_type_id) OVER (PARTITION BY vs.visit_number ORDER BY activation_time DESC) AS last_discharge_type,
    first_value(discharge_dest_code) OVER (PARTITION BY vs.visit_number ORDER BY activation_time DESC) AS last_discharge_dest,
    first_value(activation_time) OVER (PARTITION BY vs.visit_number ORDER BY activation_time) AS earliest_activation_time,
    first_value(admission_date_time) OVER (PARTITION BY vs.visit_number ORDER BY activation_time) AS earliest_admission_time,
    first_value(discharge_date_time) OVER (PARTITION BY vs.visit_number ORDER BY activation_time DESC) AS latest_discharge_time,
    source_system::VARCHAR(20) AS source_system,
    org_code::VARCHAR(5) AS org_code,
    first_value(last_edit_time) OVER (PARTITION BY vs.visit_number ORDER BY activation_time DESC) AS last_edit_time,
    convert(SMALLDATETIME, getdate()) AS updated_at
  FROM src.src_flex__visit_segment AS vs
  INNER JOIN multiple_visits AS mv
    ON vs.visit_number = mv.visit_number AND vs.visit_id = mv.visit_id
  WHERE
    total_records > 1
)
SELECT
  *
FROM merged_visits
WHERE
  visit_activation_sequence_number = 1
