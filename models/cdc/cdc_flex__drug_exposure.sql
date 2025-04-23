
MODEL (
  name lth_bronze.cdc_flex__drug_exposure,
  kind full,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('DRUG_EXPOSURE') and datasource = 'flex'
)
select
    visit_id as visit_occurrence_id,
    person_source_value,
    cast(
      cast((event_id / 864000) - 21550 as datetime) as datetime2(0)
    ) as event_datetime,
    provider_source_value as provider_id,
    flex_procedure_id,
    flex_procedure_name as procedure_source_value,
    adm_route,
    source_system,
    org_code,
    last_edit_time,
    updated_at,
    replace(dosage, ' ' + adm_route, '') as dosage
  from lth_bronze.src_flex__procedure_event sfr
  where
  sfr.last_edit_time > (
    select updated_at from cdc
  )
  and sfr.last_edit_time < (
    select dateadd(day, 10, updated_at) from cdc
  )
  and sfr.last_edit_time <= getdate()
  and kardex_group_id in (17, 43, 44)
  and event_status_id in (6, 11)