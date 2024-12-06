
MODEL (
  name lth_bronze.PAYER_PLAN_PERIOD,
  kind FULL,
  cron '@daily',
);

select
  cast(null as bigint) as payer_plan_period_id,
  cast(null as bigint) as person_id,
  cast(null as datetime) as payer_plan_period_start_date,
  cast(null as datetime) as payer_plan_period_end_date,
  cast(null as bigint) as payer_concept_id,
  cast(null as varchar(50)) as payer_source_value,
  cast(null as bigint) as payer_source_concept_id,
  cast(null as bigint) as plan_concept_id,
  cast(null as varchar(50)) as plan_source_value,
  cast(null as bigint) as plan_source_concept_id,
  cast(null as bigint) as sponsor_concept_id,
  cast(null as varchar(50)) as sponsor_source_value,
  cast(null as bigint) as sponsor_source_concept_id,
  cast(null as varchar(50)) as family_source_value,
  cast(null as bigint) as stop_reason_concept_id,
  cast(null as varchar(50)) as stop_reason_source_value,
  cast(null as bigint) as stop_reason_source_concept_id
