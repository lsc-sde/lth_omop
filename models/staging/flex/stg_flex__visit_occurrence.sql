
MODEL (
  name lth_bronze.stg_flex__visit_occurrence,
  kind FULL,
  cron '@daily',
);

select distinct
  vs.person_source_value,
  vs.visit_number,
  coalesce(earliest_activation_time, activation_time) as activation_time,
  coalesce(ft.visit_type_id, vs.visit_type_id) as visit_type_id,
  case
    when ft.visit_id is not null then ft.first_visit_id
    else vs.visit_id
  end as visit_id,
  coalesce(ft.latest_status, vs.visit_status_id) as visit_status_id,
  case
    when ft.visit_id is not null then earliest_admission_time
    else admission_date_time
  end as admission_time,
  case
    when ft.visit_id is not null then latest_discharge_time
    else discharge_date_time
  end as discharge_time,
  case
    when ft.visit_id is not null then first_attending_emp_provider_id
    else attending_emp_provider_id
  end as provider_id,
  case
    when ft.visit_id is not null then first_facility
    else facility_id
  end as facility_id,
  case
    when ft.visit_id is not null then first_admission_source
    else admission_source
  end::int as admitted_from_source_value,
  case
    when ft.visit_id is not null then last_discharge_dest
    else discharge_dest_code
  end::int as discharged_to_source_value,
  vs.source_system::varchar(20),
  vs.org_code::varchar(5),
  coalesce(ft.last_edit_time, vs.last_edit_time) as last_edit_time,
  coalesce(ft.updated_at, vs.updated_at) as updated_at
from lth_bronze.cdc_flex__visit_occurrence as vs
left join lth_bronze.stg_flex__facility_transfer as ft
  on vs.visit_number = ft.visit_number