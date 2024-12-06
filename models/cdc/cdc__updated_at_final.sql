{{
  config(
    materialized = 'table',
    )
}}

select
  'MEASUREMENT' as model,
  datasource,
  max(updated_at) as updated_at
from {{ ref('MEASUREMENT') }}
group by datasource

union all

select
  'OBSERVATION' as model,
  datasource,
  max(updated_at) as updated_at
from {{ ref('OBSERVATION') }}
group by datasource

union all

select
  'VISIT_OCCURRENCE' as model,
  'flex' as datasource,
  max(last_edit_time) as updated_at
from {{ ref('VISIT_OCCURRENCE') }}

union all

select
  'fr__mother_to_child' as model,
  'flex' as datasource,
  max(last_edit_time) as updated_at
from {{ ref('fr__mother_to_child') }}

union all

select
  'fr__specimen_to_measurement' as model,
  'swl' as datasource,
  max(last_edit_time) as updated_at
from {{ ref('fr__specimen_to_measurement') }}

union all

select
  'fr__observation_to_measurement_sl' as model,
  'swl' as datasource,
  max(last_edit_time) as updated_at
from {{ ref('fr__observation_to_measurement_sl') }}

union all

select
  'fr__measurement_to_observation_sl' as model,
  'swl' as datasource,
  max(last_edit_time) as updated_at
from {{ ref('fr__measurement_to_observation_sl') }}
