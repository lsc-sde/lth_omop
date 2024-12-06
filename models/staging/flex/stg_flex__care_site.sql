
MODEL (
  name lth_bronze.stg_flex__care_site,
  kind FULL,
  cron '@daily',
);

select
  e.*,
  p.location_source_value
from (
  select
    cast(cast(care_site_id as numeric) as bigint) as care_site_id,
    CONCAT_WS(
      ' ', area,
      case when subarea = 'NA' then null else subarea end,
      case when sub_subarea = 'NA' then null else sub_subarea end)
      as care_site_name,
    location_id as care_site_source_value,
    case
      when facility = 'Royal Preston Hospital' then '1' when
        facility = 'Chorley and South Ribble District Hospital'
        then '3'
    end as postcode,
    'Quadramed Location' as place_of_service_source_value
  from lth_bronze.src_flex__care_site_ip 

  union

  select distinct
    cast(cast(care_site_id as numeric) as bigint) as care_site_id,
    care_site_name,
    care_site_source_value,
    location_id as postcode,
    'Outpatient Clinic' as place_of_service_source_value
  from lth_bronze.src_flex__care_site_op 
) as e

left join lth_bronze.ext__postcodes as p
  on
    case
      when e.postcode = '1' then 'PR29HT' when
        e.postcode = '3'
        then 'PR71PP'
      else REPLACE(e.postcode, ' ', '')
    end
    = REPLACE(p.postcode, ' ', '')
where e.postcode is not null
