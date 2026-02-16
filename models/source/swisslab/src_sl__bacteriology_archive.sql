MODEL (
  name src.src_sl__bacteriology_archive,
  kind VIEW,
  cron '@daily'
);

SELECT
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
  'rxn' AS org_code,
  'swisslab' AS source_system,
  updated_at,
  updated_at AS last_edit_time
FROM @catalog_src.@schema_src.src_sl__bacteriology_archive
