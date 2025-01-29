
MODEL (
  name lth_bronze.care_site,
  kind FULL,
  cron '@daily',
);

select distinct
  care_site_id::bigint as care_site_id,
  care_site_name::varchar(255) as care_site_name,
  place_of_service_concept_id::bigint as place_of_service_concept_id,
  location_id::bigint as location_id,
  care_site_source_value::varchar(50) as care_site_source_value,
  place_of_service_source_value::varchar(50)
    as place_of_service_source_value,
  cs.source_system::varchar(20),
  cs.org_code::varchar(5)
from lth_bronze.vocab__care_site as cs
left join lth_bronze.location as l
  on cs.location_source_value = l.location_source_value
