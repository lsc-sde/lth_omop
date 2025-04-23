
MODEL (
  name lth_bronze.cdc_fr__specimen_to_measurement,
  kind full,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('fr__specimen_to_measurement')
),

specimen as (
select * from lth_bronze.SPECIMEN s 
where s.updated_at > (
    select dateadd(d, -5, updated_at) from cdc
  )
),

measurement as (
select * from lth_bronze.MEASUREMENT s 
where measurement_event_id in (select specimen_event_id from specimen)
)

select distinct
  1147306 as domain_concept_id_1,
  specimen_id as fact_id_1,
  1147330 as domain_concept_id_2,
  ms.measurement_id as fact_id_2,
  32669 as relationship_concept_id,
  specimen_event_id,
  ms.unique_key as measurement_key,
    @generate_surrogate_key(specimen_id,ms.measurement_id,s.updated_at) as unique_key,
  s.updated_at as last_edit_time
from specimen as s
inner join measurement as ms
  on s.specimen_event_id = ms.measurement_event_id
where
  s.updated_at > (
    select dateadd(d, -5, updated_at) from cdc
  )
  and s.updated_at < (
    select dateadd(day, 90, updated_at) from cdc
  )
  and s.updated_at <= getdate()