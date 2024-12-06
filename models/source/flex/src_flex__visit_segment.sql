
MODEL (
  name lth_bronze.src_flex__visit_segment,
  kind FULL,
  cron '@daily',
);

with source_data as (
select
  *
from @catalaog_src.@schema_src.src_flex__visit_segment
)

{% if is_incremental() %}

select
  person_source_value,
  visit_id,
  visit_number,
  visit_type_id,
  visit_subtype_id,
  visit_status_id,
  admission_source,
  facility_id,
  attending_emp_provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  discharge_type_id,
  discharge_dest_code,
  discharge_dest_value,
  last_edit_time,
  updated_at
from source_data
where last_edit_time > (select max(last_edit_time) from {{ this }})

{% else %}

select
  person_source_value,
  visit_id,
  visit_number,
  visit_type_id,
  visit_subtype_id,
  visit_status_id,
  admission_source,
  facility_id,
  attending_emp_provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  discharge_type_id,
  discharge_dest_code,
  discharge_dest_value,
  last_edit_time,
  updated_at
from source_data

{% endif %}