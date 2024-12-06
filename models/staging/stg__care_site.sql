{{
  config(
    materialized = "view",
    tags = ['care_site', 'lookup', 'dimension', 'staging'],
    docs = {
        'name': 'stg__care_site',
        'description': 'Care site view for all sources'
    }
    )
}}

with all_care_sites as (
  select
    cast(care_site_id as bigint) as care_site_id,
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
    cast(postcode as varchar),
    place_of_service_source_value,
    location_source_value
  from lth_bronze.stg_flex__care_site 

  union all

  select
    concat(40, dbo.IDGeneration(care_site_source_value)) as care_site_id,
    care_site_name,
    care_site_source_value,
    cast(postcode as varchar),
    place_of_service_source_value,
    location_source_value
  from lth_bronze.stg_sl__care_site 
)

select
  care_site_id,
  care_site_name,
  care_site_source_value,
  postcode,
  place_of_service_source_value,
  location_source_value
from all_care_sites
