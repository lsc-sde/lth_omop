{{
    config(
        tags = ['vocab', 'location']
    )
}}

select
  trim(location_id) as location_id,
  location_source_value,
  country_concept_id,
  country_source_value,
  postcode
from {{ ref('stg__location') }}
where location_id is not null
