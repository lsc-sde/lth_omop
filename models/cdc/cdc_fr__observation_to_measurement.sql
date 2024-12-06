{{
  config(
    materialized = "view",
    tags = ['omop', 'fact_relationship']
    )
}}

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at 
  where
    model in ('fr__observation_to_measurement_sl')
),

observes as (
  select
    ob.observation_id,
    ob.person_id,
    ob.observation_event_id,
    ob.observation_datetime,
    slb.isolate_event_id,
    ob.unique_key,
    ob.updated_at as last_edit_time
  from lth_bronze.OBSERVATION as ob
  inner join (
    select distinct
      source_code,
      target_concept_id,
      target_domain_id,
      [group]
    from lth_bronze.vocab__source_to_concept_map 
    where [group] = 'bacteria_observation'

  ) as m
    on ob.observation_source_value = m.source_code
  left join lth_bronze.stg_sl__bacteriology as slb
    on ob.observation_event_id = slb.measurement_event_id
  where
    slb.updated_at > (
      select dateadd(d, -5, updated_at) from cdc
    )
    and slb.updated_at < (
      select dateadd(day, 90, updated_at) from cdc
    )
    and slb.updated_at <= getdate()
    and datasource = 'swl'
),

measures as (
  select
    person_id,
    measurement_id,
    measurement_event_id,
    unique_key
  from lth_bronze.MEASUREMENT 
  where person_id in (select distinct person_id from observes)
)

select distinct
  1147304 as domain_concept_id_1,
  ob.observation_id as fact_id_1,
  1147330 as domain_concept_id_2,
  m.measurement_id as fact_id_2,
  581410 as relationship_concept_id,
  ob.unique_key as observation_key,
  m.unique_key as measurement_key, 
  {{ dbt_utils.generate_surrogate_key([
        'ob.observation_id',
        'm.measurement_id',
        'ob.last_edit_time'
        ])
    }} as unique_key,
  ob.last_edit_time
from observes as ob
inner join lth_bronze.stg_sl__bacteriology as slb
  on
    ob.isolate_event_id = slb.isolate_event_id
    and ob.observation_event_id != slb.measurement_event_id
inner join measures as m
  on
    slb.measurement_event_id = m.measurement_event_id
    and ob.person_id = m.person_id
