
MODEL (
  name lth_bronze.OBSERVATION_PERIOD,
  kind FULL,
  cron '@daily',
);

select
  cast(null as bigint) as observation_period_id,
  cast(null as bigint) as person_id,
  cast(null as date) as observation_period_start_date,
  cast(null as date) as observation_period_end_date,
  cast(null as bigint) as period_type_concept_id
