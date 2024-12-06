
MODEL (
  name lth_bronze.stg_sl__bacteriology,
  kind FULL,
  cron '@daily',
);

with merged as (
  select
    order_number,
    nhs_number,
    mrn,
    date_of_birth,
    sex,
    postcode,
    isolate_number,
    visit_number,
    location_code,
    clinician,
    clinician_code,
    order_date,
    result_date,
    site,
    qualifier,
    test,
    result_value,
    'live' as source,
    updated_at
  from lth_bronze.src_sl__bacteriology_live 
  where
    result_value not in (
      select invalid_results from lth_bronze.swisslab__invalid_results 
    )
    and result_value not like '#%'

  union all

  select
    order_number,
    nhs_number,
    mrn,
    date_of_birth,
    sex,
    postcode,
    isolate_number,
    visit_number,
    location_code,
    clinician,
    clinician_code,
    order_date,
    result_date,
    site,
    qualifier,
    test,
    result_value,
    'archive' as source,
    updated_at
  from lth_bronze.src_sl__bacteriology_archive 
  where
    result_value not in (
      select invalid_results from lth_bronze.swisslab__invalid_results 
    )
    and result_value not like '#%'
),

de_duped as (
  select
    *,
    row_number() over (
      partition by order_number,
      isolate_number,
      test order by result_date desc, source desc
    ) as sequence_id
  from merged
)

select
  dd.nhs_number,
  mrn,
  date_of_birth,
  vo.visit_id as visit_occurrence_id,
  {{ dbt_utils.generate_surrogate_key([
      'nhs_number',
      'order_number',
      'isolate_number'
      ])
  }} as isolate_event_id,
  {{ dbt_utils.generate_surrogate_key([
      'nhs_number',
      'order_number',
      'isolate_number',
      'test'
      ])
  }} as measurement_event_id,
  order_date,
  pr.provider_id,
  site,
  qualifier,
  order_number,
  case
    when test in ('Organism ID', 'Neg. culture') then result_value
    else test
  end as source_name,
  case
    when test in ('Organism ID', 'Neg. culture') then result_value
    else test
  end as source_code,
  case
    when test = 'Neg. culture' then 'Negative'
    when test = 'Organism ID' then 'Positive'
    when result_value = 'I' then 'Indeterminate'
    when result_value = 'S' then 'Sensitive'
    when result_value = 'R' then 'Resistant'
    when result_value = '+' then 'Positive'
    when result_value = '-' then 'Negative'
    else result_value
  end as value_source_value,
  null as source_value,
  null as value_as_number,
  null as unit_source_value,
  null as priority,
  dd.updated_at
from de_duped as dd
left join lth_bronze.stg__provider as pr
  on dd.clinician_code = pr.cons_org_code
left join lth_bronze.stg_flex__visit_occurrence as vo
  on dd.visit_number = vo.visit_number
where test not in (
  'Quadramed req.'
)
and sequence_id = 1
