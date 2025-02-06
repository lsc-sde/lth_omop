
MODEL (
  name lth_bronze.person,
  kind FULL,
  cron '@daily',
);

select
  p.person_id::bigint as person_id,
  p.gender_concept_id::bigint as gender_concept_id,
  datepart(year, p.birth_datetime)::int as year_of_birth,
  datepart(month, p.birth_datetime)::int as month_of_birth,
  datepart(day, p.birth_datetime)::int as day_of_birth,
  p.birth_datetime::datetime as birth_datetime,
  p.race_concept_id::bigint as race_concept_id,
  p.ethnicity_concept_id::bigint as ethnicity_concept_id,
  vl.location_id::bigint as location_id,
  pr.provider_id::bigint as provider_id,
  cs.care_site_id::bigint as care_site_id,
  p.person_source_value::varchar(50) as person_source_value,
  p.gender_source_value::varchar(50) as gender_source_value,
  null::bigint as gender_source_concept_id,
  p.race_source_value::varchar(50) as race_source_value,
  null::bigint as race_source_concept_id,
  null::varchar(50) as ethnicity_source_value,
  null::bigint as ethnicity_source_concept_id,
  p.source_system::varchar(20),
  p.org_code::varchar(5)
from lth_bronze.vocab__person as p
left join
  (select distinct
    provider_id,
    provider_source_value
  from lth_bronze.PROVIDER
  ) pr
  on p.provider_id = pr.provider_source_value
left join
  (select distinct
    care_site_id,
    care_site_source_value
  from lth_bronze.CARE_SITE
  ) cs
  on p.gp_prac_code = cs.care_site_source_value
left join lth_bronze.location as vl
  on p.mailing_code = vl.location_source_value
