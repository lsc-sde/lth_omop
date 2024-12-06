
MODEL (
  name lth_bronze.METADATA,
  kind FULL,
  cron '@daily',
);

select
  cast(null as bigint) as metadata_id,
  cast(null as bigint) as metadata_concept_id,
  cast(null as bigint) as metadata_type_concept_id,
  cast(null as varchar(250)) as name,
  cast(null as varchar(250)) as value_as_string,
  cast(null as bigint) as value_as_concept_id,
  cast(null as float) as value_as_number,
  cast(null as datetime) as metadata_date,
  cast(null as datetime) as metadata_datetime
