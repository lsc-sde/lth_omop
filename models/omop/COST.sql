
MODEL (
  name lth_bronze.COST,
  kind FULL,
  cron '@daily',
);

select
  cast(null as bigint) as cost_id,
  cast(null as bigint) as cost_event_id,
  cast(null as varchar(20)) as cost_domain_id,
  cast(null as bigint) as cost_type_concept_id,
  cast(null as bigint) as currency_concept_id,
  cast(null as float) as total_charge,
  cast(null as float) as total_cost,
  cast(null as float) as total_paid,
  cast(null as float) as paid_by_payer,
  cast(null as float) as paid_by_patient,
  cast(null as float) as paid_patient_copay,
  cast(null as float) as paid_patient_coinsurance,
  cast(null as float) as paid_patient_deductible,
  cast(null as float) as paid_by_primary,
  cast(null as float) as paid_ingredient_cost,
  cast(null as float) as paid_dispensing_fee,
  cast(null as bigint) as payer_plan_period_id,
  cast(null as float) as amount_allowed,
  cast(null as bigint) as revenue_code_concept_id,
  cast(null as varchar(50)) as revenue_code_source_value,
  cast(null as bigint) as drg_concept_id,
  cast(null as varchar(3)) as drg_source_value
