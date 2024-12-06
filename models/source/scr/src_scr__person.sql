
MODEL (
  name lth_bronze.src_scr__person,
  kind FULL,
  cron '@daily',
);

select
  person_source_value,
  nhs_number,
  mrn,
  postcode,
  sex,
  date_of_birth,
  gp_code,
  gp_practice,
  date_of_death,
  ethnicity
from @catalaog_src.@schema_src.src_scr__person
