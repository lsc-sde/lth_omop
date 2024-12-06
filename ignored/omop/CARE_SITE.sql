
MODEL (
  name lth_bronze.CARE_SITE,
  kind FULL,
  cron '@daily',
);

select distinct
  cast(care_site_id as bigint) as care_site_id,
  cast(care_site_name as varchar(255)) as care_site_name,
  cast(place_of_service_concept_id as bigint) as place_of_service_concept_id,
  cast(location_id as bigint) as location_id,
  cast(care_site_source_value as varchar(50)) as care_site_source_value,
  cast(place_of_service_source_value as varchar(50))
    as place_of_service_source_value
from lth_bronze.vocab__care_site as cs
left join lth_bronze.LOCATION as l
  on cs.location_source_value = l.location_source_value
