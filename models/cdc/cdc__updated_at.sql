
MODEL (
  name lth_bronze.cdc__updated_at,
  kind FULL,
  cron '@daily',
);

/*
* ORIGINAL CODE
* select
*   model,
*   datasource,
*   updated_at
* from
*   lth_bronze.cdc__updated_at_clone
*/

select
 model,
 datasource,
 cast('2024-12-01 00:00:00' as datetime2) as updated_at
 from lth_bronze.cdc__updated_at_default