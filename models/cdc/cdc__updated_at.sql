
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
  {{ target.database }}.{{ target.schema }}_dbt_clones.cdc__updated_at_clone
