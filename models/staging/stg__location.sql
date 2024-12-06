{{
  config(
    materialized = "table",
    tags = ['location', 'staging'],
    docs = {
        'name': 'stg__location'
    }
    )
}}

select distinct
  concat(10, dbo.IDGeneration(location_source_value)) as location_id,
  location_source_value,
  42035286 as country_concept_id,
  postcode,
  'United Kingdom of Great Britain and Northern Ireland' as country_source_value
from lth_bronze.ext__postcodes 
where location_id is not null
