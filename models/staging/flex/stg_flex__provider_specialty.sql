
MODEL (
  name lth_bronze.stg_flex__provider_specialty,
  kind FULL,
  cron '@daily',
);

select distinct
  efms.emp_provider_id,
  ms.name
from lth_bronze.src_flex__emp_facility_med_spec as efms
left join lth_bronze.src_flex__medical_specialty as ms
  on efms.physician_service_id = ms.physician_service_id
where item_nbr = 1
