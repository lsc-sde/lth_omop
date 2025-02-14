
MODEL (
  name lth_bronze.cdc__updated_at_final,
  kind FULL,
  cron '@daily',
);

select
  'VISIT_OCCURRENCE'::varchar(25) as model,
  'flex'::varchar(20) as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.visit_occurrence

union all

select
  'MEASUREMENT'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.measurement
group by source_system

union all

select
  'OBSERVATION'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.observation
group by source_system

union all

select
  'CONDITION_OCCURRENCE'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.condition_occurrence
group by source_system

union all

select
  'PROCEDURE_OCCURRENCE'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.procedure_occurrence
group by source_system