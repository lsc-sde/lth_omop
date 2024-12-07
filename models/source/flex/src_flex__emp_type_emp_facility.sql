
MODEL (
  name lth_bronze.src_flex__emp_type_emp_facility,
  kind VIEW,
  cron '@daily',
);


select
  emp_type_id,
  emp_provider_id,
  facility_id
from @catalog_src.@schema_src.src_flex__emp_type_emp_facility
