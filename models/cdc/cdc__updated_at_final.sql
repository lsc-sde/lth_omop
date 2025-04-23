
MODEL (
  name lth_bronze.cdc__updated_at_final,
  kind FULL,
  cron '@daily',
);

select
  'VISIT_OCCURRENCE'::varchar(25) as model,
  'flex'::varchar(20) as datasource,
  max(last_edit_time) as updated_at,
  max(visit_occurrence_id) as id_start_value
from lth_bronze.visit_occurrence

union all

select
  'MEASUREMENT'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at,
  max(measurement_id) as id_start_value
from lth_bronze.measurement
group by source_system

union all

select
  'OBSERVATION'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at,
  max(observation_id) as id_start_value
from lth_bronze.observation
group by source_system

union all

select
  'CONDITION_OCCURRENCE'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at,
  max(condition_occurrence_id) as id_start_value
from lth_bronze.condition_occurrence
group by source_system

union all

select
  'PROCEDURE_OCCURRENCE'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at,
  max(procedure_occurrence_id) as id_start_value
from lth_bronze.procedure_occurrence
group by source_system

union all

select
  'DRUG_EXPOSURE'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at,
  max(drug_exposure_id) as id_start_value
from lth_bronze.drug_exposure
group by source_system

union all

select
  'PERSON'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at,
  max(person_id) as id_start_value
from lth_bronze.person
group by source_system