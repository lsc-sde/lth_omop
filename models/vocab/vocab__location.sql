
MODEL (
  name lth_bronze.vocab__location,
  kind FULL,
  cron '@daily',
);

select
  trim(location_id) as location_id,
  location_source_value,
  country_concept_id,
  country_source_value,
  postcode
from lth_bronze.stg__location 
where location_id is not null
