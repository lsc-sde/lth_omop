
MODEL (
  name lth_bronze.vocab__care_site,
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
    location_source_value,
    'external' as source_system,
    'ods' as org_code
  from lth_bronze.stg_ext__care_site

  union all

  select
    care_site_id,
    care_site_name,
    care_site_source_value,
    postcode::varchar,
    place_of_service_source_value,
    location_source_value,
    source_system,
    org_code
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
  location_source_value::varchar(450),
  case
    when (
      care_site_source_value like '232~%'
      or care_site_source_value like '567~%'
      or care_site_source_value like '28~%'
      or care_site_source_value in ('28', '232', '567')
    )
    and care_site_name like '%GTD%' then '8782'
    when (
      care_site_source_value like '232~%'
      or care_site_source_value like '567~%'
      or care_site_source_value like '28~%'
      or care_site_source_value in ('28', '232', '567')
    )
    and care_site_name not like '%GTD%' then '8870'
    when place_of_service_source_value = 'Quadramed Location' then '8717'
    when place_of_service_source_value = 'Outpatient Clinic' then '8756'
    when place_of_service_source_value = 'GP Practice' then null
    when place_of_service_source_value = 'NHS Trust' then '8717'
  end::bigint as place_of_service_concept_id,
  source_system::varchar(20),
  org_code::varchar(5)
from all_care_sites