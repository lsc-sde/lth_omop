
MODEL (
  name lth_bronze.stg__result,
  kind FULL,
  cron '@daily',
);

with results_union as (
  select
    flex_patient_id as patient_id,
    visit_occurrence_id,
    try_cast(measurement_event_id as varchar(80)) as measurement_event_id,
    result_datetime,
    provider_id,
    try_cast(source_code as varchar) as source_code,
    try_cast(source_name as varchar) as source_name,
    try_cast(value_source_value as varchar) as value_source_value,
    try_cast(source_value as varchar) as source_value,
    value_as_number,
    try_cast(unit_source_value as varchar) as unit_source_value,
    try_cast(null as varchar) as priority,
    'flex' as datasource,
    last_edit_time as updated_at
  from lth_bronze.stg_flex__result 

  union all

  select
    bi_patient_id as patient_id,
    visit_occurrence_id,
    try_cast(measurement_event_id as varchar(80)) as measurement_event_id,
    bi.referral_received_date as result_datetime,
    provider_id,
    try_cast(source_code as varchar) as source_code,
    try_cast(source_name as varchar) as source_name,
    try_cast(value_source_value as varchar) as value_source_value,
    try_cast(source_value as varchar) as source_value,
    value_as_number,
    try_cast(unit_source_value as varchar) as unit_source_value,
    try_cast(priority as varchar) as priority,
    'bi' as datasource,
    updated_at
  from lth_bronze.stg_bi__referrals as bi

  union all

  select
    try_cast(nhs_number as numeric) as patient_id,
    visit_occurrence_id,
    try_cast(measurement_event_id as varchar(80)) as measurement_event_id,
    order_date as result_datetime,
    provider_id,
    try_cast(source_code as varchar) as source_code,
    try_cast(source_name as varchar) as source_name,
    try_cast(value_source_value as varchar) as value_source_value,
    try_cast(source_value as varchar) as source_value,
    value_as_number,
    try_cast(unit_source_value as varchar) as unit_source_value,
    try_cast(priority as varchar) as priority,
    'swl' as datasource,
    updated_at
  from lth_bronze.cdc_sl__bacteriology 

  union all

  select
    try_cast(nhs_number as numeric) as patient_id,
    null as visit_occurrence_id,
    {{ dbt_utils.generate_surrogate_key([
      'nhs_number',
      'care_id',
      'field',
      'value',
      'cancer_site',
      'diagnosis_date'
      ])
    }} as measurement_event_id,
    diagnosis_date as result_datetime,
    null as provider_id,
    try_cast(field as varchar) as source_code,
    try_cast(field as varchar) as source_name,
    try_cast(value as varchar) as value_source_value,
    try_cast(value as varchar) as source_value,
    null as value_as_number,
    case when field = 'Max Tumour Diameter' then 'mm' else null end as unit_source_value,
    null as priority,
    'scr' as datasource,
    updated_at
  from lth_bronze.stg_scr__result 
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
    ru.datasource,
    updated_at
  from results_union as ru
  left join person as mpi
    on
      ru.patient_id = mpi.flex_patient_id
      and ru.datasource in ('flex', 'bi')
      and mpi.id = 1
  left join
    (select distinct
      person_id,
      nhs_number
    from person) as mpi_2
    on
      ru.patient_id = mpi_2.nhs_number
      and ru.datasource in ('swl', 'scr')
)

select * from results where person_id is not null


