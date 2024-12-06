
MODEL (
  name lth_bronze.LOCATION,
  kind FULL,
  cron '@daily',
);

select distinct
  cast(location_id as bigint) as location_id,
  cast(null as varchar(50)) as address_1,
  cast(null as varchar(50)) as address_2,
  cast(null as varchar(50)) as city,
  cast(null as varchar(2)) as state,
  cast(null as varchar(9)) as zip,
  cast(null as varchar(20)) as county,
  cast(location_source_value as varchar(50)) as location_source_value,
  cast(country_concept_id as bigint) as country_concept_id,
  cast(country_source_value as varchar(80)) as country_source_value,
  cast(null as float) as latitude,
  cast(null as float) as longitude
from lth_bronze.vocab__location 
