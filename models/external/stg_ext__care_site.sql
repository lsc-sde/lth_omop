
MODEL (
  name lth_bronze.stg_ext__care_site,
  kind FULL,
  cron '@daily',
);

select
  e.*,
  p.location_source_value
from (
  select distinct
    cast(cast(care_site_id as numeric) as bigint) as care_site_id,
    care_site_name,
    care_site_source_value,
    care_site_location as postcode,
    'GP Practice' as place_of_service_source_value
  from lth_bronze.src_flex__care_site_gp 

  union

  select
    10,
    'Lancashire Teaching Hospitals',
    '2',
    'PR2 9HT' as postcode,
    'NHS Trust' as place_of_service_source_value

  union

  select
    11,
    'Royal Preston Hospital',
    '1',
    'PR2 9HT' as postcode,
    'NHS Trust' as place_of_service_source_value

  union

  select
    12,
    'Chorley District Hospital',
    '3',
    'PR7 1PP' as postcode,
    'NHS Trust' as place_of_service_source_value
) as e

left join lth_bronze.ext__postcodes as p
  on
    case
      when e.postcode = '1' then 'PR29HT' when
        e.postcode = '3'
        then 'PR71PP'
      else Replace(e.postcode, ' ', '')
    end
    = Replace(p.postcode, ' ', '')
where e.postcode is not null
