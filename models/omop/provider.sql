
MODEL (
  name lth_bronze.provider,
  kind FULL,
  cron '@daily',
);

select distinct
  provider_id::bigint as provider_id,
  provider_name::varchar(255) as provider_name,
  null::varchar(20) as npi,
  null::varchar(20) as dea,
  target_concept_id::bigint as specialty_concept_id,
  c.care_site_id::bigint as care_site_id,
  null::int as year_of_birth,
  null::bigint as gender_concept_id,
  provider_source_value::varchar(50) as provider_source_value,
  specialty_source_value::varchar(50) as specialty_source_value,
  null::bigint as specialty_source_concept_id,
  null::varchar(50) as gender_source_value,
  null::bigint as gender_source_concept_id
from lth_bronze.vocab__provider as p
left join
  (
    select *
    from lth_bronze.CARE_SITE
    where place_of_service_source_value = 'NHS Trust'
  ) as c
  on p.care_site_id::varchar = c.care_site_source_value
