
MODEL (
  name lth_bronze.stg_flex__procedure_occurrence,
  kind VIEW,
  cron '@daily',
);

with procedure_occ as (
  select
    visit_number,
    procedure_date,
    index_nbr,
    provider_source_value,
    d.last_edit_time,
    d.updated_at,
    'ukcoder' as data_source,
    replace(
      left(opcs4_code, 3) + '.' + substring(opcs4_code, 4, 10), '.X', ''
    ) as source_code
  from lth_bronze.src_flex__vtg_procedure as d
),

rad_proc_occ as (
  select
    visit_id as visit_occurrence_id,
    person_source_value as person_id,
    cast((event_id / 864000) - 21550 as varchar) as event_ts,
    @convert_event_ts_to_datetime(event_ts) as procedure_datetime,
    provider_source_value as provider_id,
    flex_procedure_id,
    flex_procedure_name as procedure_source_value,
    last_edit_time,
    updated_at,
    'flex_radiology' as data_source
  from lth_bronze.src_flex__procedure_event 
  where
    kardex_group_id = 24
    and event_status_id in (6, 11)
),

visit_detail as (
  select distinct
    visit_number,
    visit_id,
    first_visit_id,
    person_source_value
  from lth_bronze.stg_flex__facility_transfer 
)

select
  isnull(v.first_visit_id, vs.visit_id) as visit_occurrence_id,
  isnull(
    v.person_source_value, vs.person_source_value
  ) as person_source_value,
  c.procedure_date as procedure_date,
  c.procedure_date as procedure_datetime,
  c.provider_source_value as provider_id,
  c.source_code as procedure_source_value,
  c.source_code,
  c.last_edit_time,
  c.updated_at,
  data_source
from procedure_occ as c
left join
  (select distinct
    visit_number,
    first_visit_id,
    person_source_value
  from visit_detail) as v
  on c.visit_number = v.visit_number
left join
  (
    select distinct
      visit_number,
      visit_id,
      person_source_value
    from lth_bronze.src_flex__visit_segment 
    where
      visit_number not in (select distinct visit_number from visit_detail)
  ) as vs
  on
    c.visit_number = vs.visit_number
    and v.first_visit_id is null
union all
select
  isnull(v.first_visit_id, c.visit_occurrence_id),
  c.person_id as person_source_value,
  cast(procedure_datetime as date),
  procedure_datetime,
  cast(provider_id as varchar),
  procedure_source_value,
  cast(flex_procedure_id as varchar),
  last_edit_time,
  updated_at,
  data_source
from rad_proc_occ as c
left join visit_detail as v
  on c.visit_occurrence_id = v.visit_id
union all
select
  visit_id as visit_occurrence_id,
  ae_po.patient_id as person_source_value,
  cast(procedure_datetime as date) as procedure_date,
  procedure_datetime as procedure_datetime,
  cast(provider_id as varchar),
  null as procedure_source_value,
  ae_po.list as source_code,
  last_edit_time,
  updated_at,
  'ae_procedures' as data_source
from lth_bronze.stg_flex__ae_procedures as ae_po
