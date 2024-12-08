
MODEL (
  name lth_bronze.stg__location,
  kind FULL,
  cron '@daily',
);

select distinct
  -- concat(10, dbo.IDGeneration(location_source_value)) as location_id,
  postcode::VARCHAR(10) as location_id,
  location_source_value,
  42035286 as country_concept_id,
  postcode::VARCHAR(10),
  'United Kingdom of Great Britain and Northern Ireland'::VARCHAR(255) as country_source_value
from lth_bronze.ext__postcodes 
where location_id is not null
