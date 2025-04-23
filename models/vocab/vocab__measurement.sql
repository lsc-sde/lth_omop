
MODEL (
  name lth_bronze.vocab__measurement,
  kind view,
  cron '@daily',
);

select distinct
  r.person_id,
  r.visit_occurrence_id,
  null as visit_detail_id,
  r.measurement_event_id::varchar(80),
  r.provider_id,
  r.result_datetime,
  cm.target_concept_id,
  32817 as type_concept_id,
  cast(
    replace(replace(r.source_value, '<', ''), '>', '') as float
  ) as value_as_number,
  coalesce(dc.target_concept_id, rs.concept_id) as value_as_concept_id,
  um.target_concept_id as unit_concept_id,
  null as range_low,
  null as range_high,
  r.source_name as source_value,
  null as measurement_source_concept_id,
  isnull(r.unit_source_value, um.source_code_description) as unit_source_value,
  null as unit_source_concept_id,
  r.value_source_value as value_source_value,
  null as meas_event_field_concept_id,
  case
    when r.source_value like '%>%' then 4172704
    when r.source_value like '%<%' then 4171756
  end as operator_concept_id,
  r.org_code,
  r.source_system,
  r.updated_at
from lth_bronze.stg__result as r
inner join
  (
    select distinct
      source_code,
      target_concept_id,
      target_domain_id,
      concept_group
    from lth_bronze.vocab__source_to_concept_map
    where
      concept_group = 'result'
      or (
        (
          concept_group = 'bacteria_presence'
          or concept_group = 'bacteria_sensitivities'
          or concept_group = 'bacteriology_other_test'

        )
        and source_system = 'swisslab'
      )
  ) as cm
  on r.source_code = cm.source_code
left join
  (
    select
      source_code,
      source_code_description,
      target_concept_id,
      target_domain_id
    from lth_bronze.vocab__source_to_concept_map
    where concept_group = 'units'
  ) as um
  on r.source_code = um.source_code
left join
  (
    select
      target_concept_id,
      source_code,
      source_code_description
    from lth_bronze.vocab__source_to_concept_map
    where concept_group in ('decoded', 'bacteriology_other_result')
  ) as dc
  on
    r.source_code = dc.source_code
    and r.value_source_value = dc.source_code_description
left join
  (
    select
      concept_id,
      concept_name
    from @catalog_src.@schema_vocab.CONCEPT
    where
      concept_name in (
        'Positive', 'Negative', 'Sensitive', 'Indeterminate', 'Resistant'
      )
      and domain_id in ('Meas Value')
      and vocabulary_id in ('SNOMED')
  ) as rs
  on
    r.value_source_value = rs.concept_name
where
  cm.target_domain_id = 'Measurement'

union

select distinct
  r.person_id,
  r.visit_occurrence_id,
  null as visit_detail_id,
  r.measurement_event_id::varchar(80),
  r.provider_id,
  r.result_datetime,
  c.target_concept_id,
  32817 as type_concept_id,
  try_cast(
    replace(replace(r.source_value, '<', ''), '>', '') as float
  ) as value_as_number,
  c2.target_concept_id as value_as_concept_id,
  null as unit_concept_id,
  null as range_low,
  null as range_high,
  r.source_name as source_value,
  null as measurement_source_concept_id,
  r.unit_source_value as unit_source_value,
  case when unit_source_value = 'mm' then 8587 end
    as unit_source_concept_id,
  r.value_source_value as value_source_value,
  null as meas_event_field_concept_id,
  case
    when r.source_value like '%>%' then 4172704
    when r.source_value like '%<%' then 4171756
  end as operator_concept_id,
  r.org_code::varchar(5),
  r.source_system,
  r.updated_at
from lth_bronze.stg__result as r
left join
  (
    select
      source_code,
      source_code_description,
      target_concept_id,
      target_domain_id
    from lth_bronze.vocab__source_to_concept_map
    where concept_group = 'scr_fields'
  ) as c
  on r.source_code = c.source_code
left join
  (
    select
      source_code,
      source_code_description,
      target_concept_id,
      target_domain_id
    from lth_bronze.vocab__source_to_concept_map
    where concept_group = 'scr_results'
  ) as c2
  on
    r.value_source_value = c2.source_code_description
    and r.source_name = c2.source_code
left join @catalog_src.@schema_vocab.CONCEPT as cn
  on c.target_concept_id = cn.concept_id
where
  source_system = 'scr'
  and cn.domain_id = 'Measurement'