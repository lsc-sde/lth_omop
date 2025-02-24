
MODEL (
  name lth_bronze.cdc__updated_at,
  kind view,
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
 model::varchar(50),
 datasource::varchar(20),
 updated_at::datetime,
 id_start_value::int
from lth_bronze.cdc__updated_at_clone