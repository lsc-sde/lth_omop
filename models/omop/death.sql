
MODEL (
  name lth_bronze.death,
  kind FULL,
  cron '@daily',
);

select
  person_id::bigint as person_id,
  death_date::date as death_date,
  death_datetime::datetime as death_datetime,
  death_type_concept_id::bigint as death_type_concept_id,
  null::bigint as cause_concept_id,
  null::varchar(50) as cause_source_value,
  null::bigint as cause_source_concept_id,
  source_system::varchar(20),
  org_code::varchar(5)
from lth_bronze.vocab__death
