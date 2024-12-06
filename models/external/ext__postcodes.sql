
MODEL (
  name lth_bronze.ext__postcodes,
  kind FULL,
  cron '@daily',
);
{# ToDo: The source for postcodes data is hardcoded and needs to be ref. #}
select
  p.pcd7 as postcode,
  p.lsoa21cd as location_source_value,
  cast(dense_rank() over (order by p.lsoa21cd) as bigint) as location_id
from dbt_omop.admin.postcodes as p
