
MODEL (
  name lth_bronze.stg_flex__provider_partial,
  kind FULL,
  cron '@daily',
);

select distinct
  ep.name as provider_name,
  2 as care_site_id,
  ep.emp_provider_id as provider_source_value,
  coalesce(emp_med_spec.name, et.name) as specialty_source_value,
  row_number() over (
    partition by ep.emp_provider_id
    order by
      case
        when
          coalesce(emp_med_spec.name, et.name) like '%Nurse%'
          or coalesce(
            emp_med_spec.name, et.name
          ) like '%Consultant%'
          or coalesce(
            emp_med_spec.name, et.name
          ) like '%Pharmacist%'
          or coalesce(
            emp_med_spec.name, et.name
          ) like '%Radiographer%'
          then 1
        else 0
      end desc
  ) as identifier
from lth_bronze.src_flex__emp_provider as ep
left join lth_bronze.stg_flex__provider_specialty as emp_med_spec
  on ep.emp_provider_id = emp_med_spec.emp_provider_id
left join lth_bronze.stg_flex__emp_type_emp_facility as e
  on ep.emp_provider_id = e.emp_provider_id
left join lth_bronze.src_flex__emp_type as et
  on e.emp_type_id = et.emp_type_id
