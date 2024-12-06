{{
  config(
    materialized = "table",
    tags = ['omop', 'provider']
    )
}}

select distinct
  cast(provider_id as bigint) as provider_id,
  cast(provider_name as varchar(255)) as provider_name,
  cast(null as varchar(20)) as npi,
  cast(null as varchar(20)) as dea,
  cast(target_concept_id as bigint) as specialty_concept_id,
  cast(c.care_site_id as bigint) as care_site_id,
  cast(null as int) as year_of_birth,
  cast(null as bigint) as gender_concept_id,
  cast(provider_source_value as varchar(50)) as provider_source_value,
  cast(specialty_source_value as varchar(50)) as specialty_source_value,
  cast(null as bigint) as specialty_source_concept_id,
  cast(null as varchar(50)) as gender_source_value,
  cast(null as bigint) as gender_source_concept_id
from {{ ref('vocab__provider') }} as p
left join
  (
    select *
    from {{ ref('CARE_SITE') }}
    where place_of_service_source_value = 'NHS Trust'
  ) as c
  on cast(p.care_site_id as varchar) = c.care_site_source_value
