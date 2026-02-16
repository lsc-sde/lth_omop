MODEL (
  name ext.ext__postcodes,
  kind FULL,
  cron '@daily'
);

/* ToDo: The source for postcodes data is hardcoded and needs to be ref. */
SELECT
  p.pcd7 AS postcode,
  p.lsoa21cd AS location_source_value
FROM dbt_omop.admin.postcodes AS p
