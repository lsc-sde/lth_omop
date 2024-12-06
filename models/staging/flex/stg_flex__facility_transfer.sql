{{
  config(
    materialized = "table",
    tags = ['bi', 'flex', 'staging', 'visit']
    )
}}

with multiple_visits as (
  select distinct
    visit_number,
    visit_id,
    COUNT(*) over (partition by visit_number) as total_records
  from {{ ref('src_flex__visit_segment') }}
  group by
    visit_number,
    visit_id
),

merged_visits as (
  select
    vs.visit_id,
    vs.visit_number,
    person_source_value,
    FIRST_VALUE(
      visit_type_id
    ) over (
      partition by vs.visit_number order by activation_time
    ) as visit_type_id,
    COUNT(*) over (partition by vs.visit_number) as total_activations,
    COUNT(*) over (partition by vs.visit_id) as total_entries,
    row_number() over (
      partition by vs.visit_number, vs.visit_id order by activation_time
    ) as visit_activation_sequence_number,
    row_number() over (
      partition by vs.visit_number order by activation_time
    ) as activation_sequence_number,
    FIRST_VALUE(
      vs.visit_id
    ) over (
      partition by vs.visit_number order by activation_time
    ) as first_visit_id,
    FIRST_VALUE(
      attending_emp_provider_id
    ) over (
      partition by vs.visit_number order by activation_time
    ) as first_attending_emp_provider_id,
    FIRST_VALUE(
      facility_id
    ) over (
      partition by vs.visit_number order by activation_time
    ) as first_facility,
    FIRST_VALUE(
      visit_status_id
    ) over (
      partition by vs.visit_number order by activation_time desc
    ) as latest_status,
    FIRST_VALUE(
      admission_source
    ) over (
      partition by vs.visit_number order by activation_time
    ) as first_admission_source,
    FIRST_VALUE(
      discharge_type_id
    ) over (
      partition by vs.visit_number order by activation_time desc
    ) as last_discharge_type,
    FIRST_VALUE(
      discharge_dest_code
    ) over (
      partition by vs.visit_number order by activation_time desc
    ) as last_discharge_dest,
    FIRST_VALUE(
      activation_time
    ) over (
      partition by vs.visit_number order by activation_time
    ) as earliest_activation_time,
    FIRST_VALUE(
      admission_date_time
    ) over (
      partition by vs.visit_number order by activation_time
    ) as earliest_admission_time,
    FIRST_VALUE(
      discharge_date_time
    ) over (
      partition by vs.visit_number order by activation_time desc
    ) as latest_discharge_time,
    FIRST_VALUE(
      last_edit_time
    ) over (
      partition by vs.visit_number order by activation_time desc
    ) as last_edit_time,
    convert(smalldatetime, SYSDATETIME()) as updated_at
  from {{ ref('src_flex__visit_segment') }} as vs
  inner join multiple_visits as mv
    on
      vs.visit_number = mv.visit_number
      and vs.visit_id = mv.visit_id
  where
    total_records > 1
)

select * from merged_visits where visit_activation_sequence_number = 1
