
MODEL (
  name lth_bronze.stg_flex__result,
  kind FULL,
  cron '@daily',
);

with visit_detail as (
  select distinct
    visit_number,
    visit_id,
    first_visit_id,
    person_source_value
  from lth_bronze.stg_flex__facility_transfer
)

select distinct
  fr.person_source_value as flex_patient_id,
  fr.event_date_time as result_datetime,
  fr.emp_provider_id as provider_id,
  fr.data_element_id as source_code,
  fr.field_name as source_name,
  fr.procedure_name,
  fr.result_value as value_source_value,
  fr.trimmed_result_value as source_value,
  cast(
    case
      when
        result_value like 'Score %' and len(result_value) = 7
        then replace(result_value, 'Score ', '')
      else replace(replace(fr.trimmed_result_value, '<', ''), '>', '')
    end as float
  ) as value_as_number,
  fr.display_unit as unit_source_value,
  source_system::varchar(20),
  org_code::varchar(5),
  fr.last_edit_time,
  fr.updated_at,
  isnull(vd.first_visit_id, fr.visit_id)::numeric(12,0) as visit_occurrence_id,
  concat(fr.visit_id, fr.event_id)::varchar(82) as measurement_event_id
from lth_bronze.cdc_flex__result as fr
left join visit_detail as vd
  on fr.visit_id = vd.visit_id