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
    model in ('fr__measurement_to_observation_sl')
),

measures as (
  select
    ms.measurement_id,
    ms.person_id,
    ms.measurement_event_id,
    ms.measurement_datetime,
    ms.updated_at as last_edit_time,
    ms.unique_key
  from lth_bronze.MEASUREMENT as ms
  inner join (
    select distinct
      source_code,
      target_concept_id,
      target_domain_id,
      [group]
    from lth_bronze.vocab__source_to_concept_map 
    where
      (
        [group] = 'bacteria_presence'
        or [group] = 'bacteriology_other_test'
      )

  ) as m
    on ms.measurement_source_value = m.source_code
  left join lth_bronze.stg_sl__bacteriology as slb
    on ms.measurement_event_id = slb.measurement_event_id
  where datasource = 'swl'
    and slb.updated_at > (
      select dateadd(d, -5, updated_at) from cdc
    )
    and slb.updated_at < (
      select dateadd(day, 90, updated_at) from cdc
    )
    and slb.updated_at <= getdate()
    and datasource = 'swl'
),

observes as (
  select
    person_id,
    observation_id,
    observation_event_id,
    unique_key
  from lth_bronze.OBSERVATION 
  where person_id in (select distinct person_id from measures)
)

select distinct
  1147330 as domain_concept_id_1,
  ms.measurement_id as fact_id_1,
  1147304 as domain_concept_id_2,
  ob.observation_id as fact_id_2,
  581411 as relationship_concept_id,
  ob.unique_key as observation_key,
  ms.unique_key as measurement_key,
  {{ dbt_utils.generate_surrogate_key([
        'ms.measurement_id',
        'ob.observation_id',
        'ms.last_edit_time'
        ])
    }} as unique_key,
  ms.last_edit_time
from measures as ms
inner join lth_bronze.stg_sl__bacteriology as slb
  on ms.measurement_event_id = slb.measurement_event_id
inner join observes as ob
  on
    slb.measurement_event_id = ob.observation_event_id
    and ob.person_id = ms.person_id
