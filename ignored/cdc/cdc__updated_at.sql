
MODEL (
  name lth_bronze.cdc__updated_at,
  kind FULL,
  cron '@daily',
);
select
  model,
  datasource,
  updated_at
from
  lth_bronze.cdc__updated_at_clone
