
MODEL (
  name lth_bronze.cdc__updated_at_final,
  kind FULL,
  cron '@daily',
);

select
  'VISIT_OCCURRENCE'::varchar(25) as model,
  'flex'::varchar(20) as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.VISIT_OCCURRENCE

union all

select
  'MEASUREMENT'::varchar(25) as model,
  source_system::varchar(20) as datasource,
  max(last_edit_time) as updated_at
from lth_bronze.MEASUREMENT
group by source_system