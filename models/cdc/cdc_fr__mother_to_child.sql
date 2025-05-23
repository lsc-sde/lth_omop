
MODEL (
  name lth_bronze.cdc_fr__mother_to_child,
  kind full,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('fr__mother_to_child')
)

select
  1147314 as domain_concept_id_1,
  c.person_id as fact_id_1,
  1147314 as domain_concept_id_2,
  m.person_id as fact_id_2,
  40478925 as relationship_concept_id,
  @generate_surrogate_key(c.person_id,m.person_id) as unique_key,
  c.last_edit_time
from lth_bronze.stg__person as c
inner join lth_bronze.stg__person as m
  on c.mother_person_source_value = m.person_source_value
where
  c.last_edit_time > (
    select dateadd(d, -1, updated_at) from cdc
  )
  and c.last_edit_time < (
    select dateadd(day, 90, updated_at) from cdc
  )
  and c.last_edit_time <= getdate()