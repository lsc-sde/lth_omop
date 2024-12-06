
MODEL (
  name lth_bronze.era__condition_target,
  kind FULL,
  cron '@daily',
);

select
  co.PERSON_ID,
  co.condition_concept_id,
  co.CONDITION_START_DATE,
  coalesce(co.CONDITION_END_DATE, DATEADD(day, 1, co.CONDITION_START_DATE))
    as CONDITION_END_DATE
from lth_bronze.CONDITION_OCCURRENCE as co
