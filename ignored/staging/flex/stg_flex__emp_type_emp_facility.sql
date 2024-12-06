
MODEL (
  name lth_bronze.stg_flex__emp_type_emp_facility,
  kind FULL,
  cron '@daily',
);

select distinct
  emp_provider_id,
  emp_type_id
from lth_bronze.src_flex__emp_type_emp_facility 
