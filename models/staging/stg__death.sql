
MODEL (
  name lth_bronze.stg__death,
  kind FULL,
  cron '@daily',
);

select
  person_id,
  person_source_value,
  death_datetime
from lth_bronze.stg__person 
where death_datetime is not null
