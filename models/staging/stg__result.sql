
MODEL (
  name lth_bronze.stg__result,
  kind FULL,
  cron '@daily',
);

with results_union as (
  select
    flex_patient_id as patient_id,
    visit_occurrence_id,
    measurement_event_id::varchar as measurement_event_id,
    result_datetime,
    provider_id,
    try_cast(source_code as varchar) as source_code,
    try_cast(source_name as varchar) as source_name,
    try_cast(value_source_value as varchar) as value_source_value,
    try_cast(source_value as varchar) as source_value,
    value_as_number,
    try_cast(unit_source_value as varchar) as unit_source_value,
    try_cast(null as varchar) as priority,
    org_code,
	  source_system,
    last_edit_time as updated_at
  from lth_bronze.stg_flex__result

  union all

  select
    bi_patient_id as patient_id,
    visit_occurrence_id,
    measurement_event_id::varchar as measurement_event_id,
    bi.referral_received_date as result_datetime,
    provider_id,
    try_cast(source_code as varchar) as source_code,
    try_cast(source_name as varchar) as source_name,
    try_cast(value_source_value as varchar) as value_source_value,
    try_cast(source_value as varchar) as source_value,
    value_as_number,
    try_cast(unit_source_value as varchar) as unit_source_value,
    try_cast(priority as varchar) as priority,    
    org_code,
	  source_system,
    updated_at
  from lth_bronze.stg_bi__referrals as bi

  union all

  select
    try_cast(nhs_number as numeric) as patient_id,
    visit_occurrence_id,
    measurement_event_id as measurement_event_id,
    order_date as result_datetime,
    provider_id,
    try_cast(source_code as varchar) as source_code,
    try_cast(source_name as varchar) as source_name,
    try_cast(value_source_value as varchar) as value_source_value,
    try_cast(source_value as varchar) as source_value,
    value_as_number,
    try_cast(unit_source_value as varchar) as unit_source_value,
    try_cast(priority as varchar) as priority,
    org_code,
	  source_system,
    updated_at
  from lth_bronze.cdc_sl__bacteriology
),

person as (
select
    *,
    row_number() over (partition by person_id order by last_edit_time desc) as id
from lth_bronze.stg__master_patient_index
),

results as (
  select
    coalesce(mpi.person_id, mpi_2.person_id) as person_id,
    visit_occurrence_id,
    ru.measurement_event_id,
    ru.provider_id,
    ru.result_datetime,
    ru.value_as_number,
    ru.source_name,
    ru.source_code,
    ru.unit_source_value,
    ru.value_source_value,
    ru.source_value,
    ru.priority,
    ru.org_code,
    ru.source_system,
    updated_at
  from results_union as ru
  left join person as mpi
    on
      ru.patient_id = mpi.flex_patient_id
      and ru.source_system in ('flex', 'bi')
      and mpi.id = 1
  left join
    (select distinct
      person_id,
      nhs_number
    from person) as mpi_2
    on
      ru.patient_id = mpi_2.nhs_number
      and ru.source_system in ('swl')
)

select * from results where person_id is not null