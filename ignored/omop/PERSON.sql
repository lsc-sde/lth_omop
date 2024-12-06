
MODEL (
  name lth_bronze.PERSON,
  kind FULL,
  cron '@daily',
);

select
  cast(p.person_id as bigint) as person_id,
  cast(p.gender_concept_id as bigint) as gender_concept_id,
  cast(datepart(year, p.birth_datetime) as int) as year_of_birth,
  cast(datepart(month, p.birth_datetime) as int) as month_of_birth,
  cast(datepart(day, p.birth_datetime) as int) as day_of_birth,
  cast(p.birth_datetime as datetime) as birth_datetime,
  cast(p.race_concept_id as bigint) as race_concept_id,
  cast(p.ethnicity_concept_id as bigint) as ethnicity_concept_id,
  cast(vl.location_id as bigint) as location_id,
  cast(pr.provider_id as bigint) as provider_id,
  cast(cs.care_site_id as bigint) as care_site_id,
  cast(p.person_source_value as varchar(50)) as person_source_value,
  cast(p.gender_source_value as varchar(50)) as gender_source_value,
  cast(null as bigint) as gender_source_concept_id,
  cast(p.race_source_value as varchar(50)) as race_source_value,
  cast(null as bigint) as race_source_concept_id,
  cast(null as varchar(50)) as ethnicity_source_value,
  cast(null as bigint) as ethnicity_source_concept_id
from lth_bronze.vocab__person as p
left join
  (select distinct
    provider_id,
    provider_source_value
  from lth_bronze.PROVIDER as pr
  on p.provider_id = pr.provider_source_value
left join
  (select distinct
    care_site_id,
    care_site_source_value
  from lth_bronze.CARE_SITE as cs
  on p.gp_prac_code = cs.care_site_source_value
left join lth_bronze.vocab__location as vl
  on p.mailing_code = vl.postcode
