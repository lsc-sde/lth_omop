
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
 '2024-12-01' as updated_at
 from lth_bronze.cdc__updated_at_default