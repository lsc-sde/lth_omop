
MODEL (
  name lth_bronze.stg_flex__provider,
  kind FULL,
  cron '@daily',
);

with provider_base as (
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
    ) as identifier,
    ep.source_system,
    ep.org_code
  from lth_bronze.src_flex__emp_provider as ep
  left join lth_bronze.stg_flex__provider_specialty as emp_med_spec
    on ep.emp_provider_id = emp_med_spec.emp_provider_id
  left join lth_bronze.stg_flex__emp_type_emp_facility as e
    on ep.emp_provider_id = e.emp_provider_id
  left join lth_bronze.src_flex__emp_type as et
    on e.emp_type_id = et.emp_type_id
)

select
  provider_name,
  care_site_id,
  cast(provider_source_value as varchar) as provider_source_value,
  specialty_source_value,
  c.cons_org_code,
  row_number() over (order by newid()) as provider_id,  
  p.source_system::varchar(20),
  p.org_code::varchar(5)
from provider_base as p
left join lth_bronze.src_flex__emp_consultant as c
  on p.provider_source_value = c.cons_emp_provider_id
where p.identifier = 1
