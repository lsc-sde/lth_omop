
MODEL (
  name lth_bronze.stg_flex__emp_type_emp_facility,
  kind FULL,
  cron '@daily',
);

select distinct
  emp_provider_id,
  emp_type_id,
  source_system::varchar(20),
  org_code::varchar(5)
from lth_bronze.src_flex__emp_type_emp_facility 
