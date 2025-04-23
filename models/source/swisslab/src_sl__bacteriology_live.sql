
MODEL (
  name lth_bronze.src_sl__bacteriology_live,
  kind VIEW,
  cron '@daily',
);

select
  order_number,
  nhs_number,
  mrn,
  date_of_birth,
  postcode,
  sex,
  isolate_number,
  visit_number,
  location_code,
  clinician,
  clinician_code,
  order_date,
  result_date,
  site,
  qualifier,
  test,
  result_value,
  'rxn' as org_code,
  'swisslab' as source_system,
  updated_at
from @catalog_src.@schema_src.src_sl__bacteriology_live