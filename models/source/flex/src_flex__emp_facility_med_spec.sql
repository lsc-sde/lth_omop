
MODEL (
  name lth_bronze.src_flex__emp_facility_med_spec,
  kind FULL,
  cron '@daily',
);

select
  emp_provider_id,
  facility_id,
  physician_service_id,
  item_nbr
from @catalog_src.@schema_src.src_flex__emp_facility_med_spec
