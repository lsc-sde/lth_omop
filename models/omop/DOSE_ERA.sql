
MODEL (
  name lth_bronze.DOSE_ERA,
  kind FULL,
  cron '@daily',
);

select
  cast(null as bigint) as dose_era_id,
  cast(null as bigint) as person_id,
  cast(null as bigint) as drug_concept_id,
  cast(null as bigint) as unit_concept_id,
  cast(null as float) as dose_value,
  cast(null as datetime) as dose_era_start_date,
  cast(null as datetime) as dose_era_end_date
