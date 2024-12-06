
MODEL (
  name lth_bronze.cdc__updated_at_final,
  kind FULL,
  cron '@daily',
);

select
  'MEASUREMENT' as model,
  datasource,
  max(updated_at) as updated_at
from lth_bronze.MEASUREMENT 
group by datasource

union all

select
  'OBSERVATION' as model,
  datasource,
  max(updated_at) as updated_at
from lth_bronze.OBSERVATION 
group by datasource

union all

select
  'VISIT_OCCURRENCE' as model,
  'flex' as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.VISIT_OCCURRENCE 

union all

select
  'fr__mother_to_child' as model,
  'flex' as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.fr__mother_to_child 

union all

select
  'fr__specimen_to_measurement' as model,
  'swl' as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.fr__specimen_to_measurement 

union all

select
  'fr__observation_to_measurement_sl' as model,
  'swl' as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.fr__observation_to_measurement_sl 

union all

select
  'fr__measurement_to_observation_sl' as model,
  'swl' as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.fr__measurement_to_observation_sl 
