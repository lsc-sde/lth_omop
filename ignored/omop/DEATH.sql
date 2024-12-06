
MODEL (
  name lth_bronze.DEATH,
  kind FULL,
  cron '@daily',
);

select
  cast(person_id as bigint) as person_id,
  cast(death_date as date) as death_date,
  cast(death_datetime as datetime) as death_datetime,
  cast(death_type_concept_id as bigint) as death_type_concept_id,
  cast(null as bigint) as cause_concept_id,
  cast(null as varchar(50)) as cause_source_value,
  cast(null as bigint) as cause_source_concept_id
from lth_bronze.vocab__death 
