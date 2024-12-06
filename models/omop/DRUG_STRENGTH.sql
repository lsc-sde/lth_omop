{{
  config(
    materialized = "table",
    tags = ['omop', 'drugs']
    )
}}
select
  cast(null as bigint) as drug_concept_id,
  cast(null as bigint) as ingredient_concept_id,
  cast(null as float) as amount_value,
  cast(null as bigint) as amount_unit_concept_id,
  cast(null as float) as numerator_value,
  cast(null as bigint) as numerator_unit_concept_id,
  cast(null as float) as denominator_value,
  cast(null as bigint) as denominator_unit_concept_id,
  cast(null as int) as box_size,
  cast(null as date) as valid_start_date,
  cast(null as date) as valid_end_date,
  cast(null as varchar(1)) as invalid_reason
