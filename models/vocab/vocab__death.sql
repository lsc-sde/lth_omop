
MODEL (
  name lth_bronze.vocab__death,
  kind FULL,
  cron '@daily',
);

select
  person_id,
  death_datetime,
  32817 as death_type_concept_id,
  death_datetime::date as death_date,
  source_system,
  org_code
from lth_bronze.stg__death
where death_datetime is not null