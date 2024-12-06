{{
    config(
        tags = ['staging', 'sl', 'care_site'],
        unique_key=['care_id','plan_id']
    )
}}

select
  e.care_site_name,
  e.care_site_source_value,
  e.postcode,
  e.place_of_service_source_value,
  p.location_source_value
from (
  select distinct
    Description as care_site_name,
    Location_Code as care_site_source_value,
    null as postcode,
    'Swisslab' as place_of_service_source_value
  from {{ ref('swisslab__locations') }}
  where Quadramed_Code is null
) as e

left join {{ ref('ext__postcodes') }} as p
  on
    case
      when e.postcode = '1' then 'PR29HT'
      when e.postcode = '3' then 'PR71PP'
      else Replace(e.postcode, ' ', '')
    end
    = Replace(p.postcode, ' ', '')
