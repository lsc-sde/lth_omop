
MODEL (
  name lth_bronze.stg__care_site,
  kind FULL,
  cron '@daily',
);

with all_care_sites as (
  select
    care_site_id::bigint as care_site_id,
    care_site_name,
    care_site_source_value,
    postcode,
    place_of_service_source_value,
    location_source_value
  from lth_bronze.stg_ext__care_site

  union all

  select
    care_site_id,
    care_site_name,
    care_site_source_value,
    postcode::varchar,
    place_of_service_source_value,
    location_source_value
  from lth_bronze.stg_flex__care_site

--  union all

--  select
--    concat(40, dbo.IDGeneration(care_site_source_value)) as care_site_id,
--    care_site_name,
--    care_site_source_value,
--    postcode::varchar,
--    place_of_service_source_value,
--    location_source_value
--  from lth_bronze.stg_sl__care_site
)

select
  care_site_id::bigint,
  care_site_name::varchar(450),
  care_site_source_value::varchar(450),
  postcode::varchar(15),
  place_of_service_source_value::varchar(450),
  location_source_value::varchar(450)
from all_care_sites
