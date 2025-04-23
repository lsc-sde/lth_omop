
MODEL (
  name lth_bronze.stg__master_patient_index,
  kind FULL,
  cron '@daily',
);

with flex_person as (
  select
    person_source_value as flex_patient_id,
    mrn as flex_mrn,
    cast(nhs_number as numeric(10, 0)) as nhs_number,
    birth_datetime,
    death_datetime,
    last_edit_time,
    source_system,
	org_code
  from lth_bronze.src_flex__person
  group by
    person_source_value,
    mrn,
    birth_datetime,
    death_datetime,
    nhs_number,
    last_edit_time,
    source_system,
	org_code
),

scr_person as (
  select
    person_source_value as scr_patient_id,
    mrn as flex_mrn,
    cast(nhs_number as numeric(10, 0)) as nhs_number,
    'scr' as data_source,
    cast(date_of_birth as datetime) as birth_datetime,
    cast(date_of_death as datetime) as death_datetime,
    null as last_edit_time,
    source_system,
	org_code
  from lth_bronze.src_scr__person
  group by
    person_source_value,
    mrn,
    date_of_birth,
    date_of_death,
    nhs_number,
    source_system,
	org_code
),

sl_person as (
--  select
--    null as sl_patient_id,
--    mrn as flex_mrn,
--    try_cast(nhs_number as numeric) as nhs_number,
--    'sl' as data_source,
--    cast(date_of_birth as datetime) as birth_datetime,
--    null as death_datetime,
--    null as last_edit_time
--  from stg_sl__bacteriology
--  group by
--    mrn,
--    date_of_birth,
--    nhs_number,
--    updated_at
select top(1)
    null as sl_patient_id,
    null as flex_mrn,
    null as nhs_number,
    null as data_source,
    null as birth_datetime,
    null as death_datetime,
    null as last_edit_time,
	null as source_system,
	null as org_code
),

nhs_numbers as (
  select nhs_number
  from flex_person
  where nhs_number is not null
  union
  select distinct nhs_number
  from scr_person
  where nhs_number is not null
  union
  select distinct nhs_number
  from sl_person
  where nhs_number is not null
),

mpi_base as (
  select distinct
    nn.nhs_number,
    fp.flex_patient_id,
    fp.flex_mrn,
    sp.scr_patient_id,
    coalesce(fp.birth_datetime, sp.birth_datetime) as birth_datetime,
    coalesce(fp.death_datetime, sp.death_datetime) as death_datetime,
    fp.last_edit_time,
    coalesce(fp.source_system, sp.source_system) as source_system,
	coalesce(fp.org_code, sp.org_code) as org_code
  from nhs_numbers as nn
  left join flex_person as fp
    on nn.nhs_number = fp.nhs_number
  left join scr_person as sp
    on nn.nhs_number = sp.nhs_number
),

mpi_base_flex as (
  select *
  from mpi_base
  union all
  select
    nhs_number,
    fp.flex_patient_id,
    fp.flex_mrn,
    null as scr_patient_id,
    fp.birth_datetime,
    death_datetime,
    last_edit_time,
	source_system,
	org_code
  from flex_person as fp
  where
    flex_patient_id not in (
      select distinct flex_patient_id
      from mpi_base
      where flex_patient_id is not null
    )
),

mpi_base_scr as (
  select *
  from mpi_base_flex
  union all
  select
    nhs_number,
    null as flex_patient_id,
    null as flex_mrn,
    scr_patient_id,
    birth_datetime,
    death_datetime,
    last_edit_time,
	source_system,
	org_code
  from scr_person
  where
    scr_patient_id not in (
      select distinct scr_patient_id
      from mpi_base_flex
      where scr_patient_id is not null
    )
),

mpi_base_sl as (
  select *
  from mpi_base_flex
  union all
  select
    nhs_number,
    null as flex_patient_id,
    null as flex_mrn,
    null as scr_patient_id,
    birth_datetime,
    death_datetime,
    last_edit_time,
	source_system,
	org_code
  from sl_person
  where
    nhs_number not in (
      select distinct nhs_number
      from mpi_base_scr
    )
),

mpi_final_base as (
  select
    mpi.*,
    case
      when
        mpi.flex_mrn is not null or mpi.flex_patient_id is not null
        then 'flex'
      when mpi.scr_patient_id is not null then 'scr'
      else 'sl'
    end as source,
    count(mpi.nhs_number)
      over (partition by mpi.nhs_number)
      as nhs_number_count,
    count(mpi.flex_patient_id)
      over (partition by mpi.flex_patient_id)
      as flex_patient_id_count,
    count(mpi.flex_mrn) over (partition by mpi.flex_mrn) as flex_mrn_count,
    count(mpi.scr_patient_id)
      over (partition by mpi.scr_patient_id)
      as scr_patient_id_count
  from mpi_base_sl as mpi
  where mpi.flex_patient_id is not null
),

mpi_final_all as (
  select
    cast(
      translate(
        cast(
          coalesce(p.collapsed_into_patient_id, mpi.flex_patient_id) as varchar
        ),
        '1234567890',
        '9876543210'
      )
      as bigint
    ) as person_id,
    coalesce(p.collapsed_into_patient_id, mpi.flex_patient_id)
      as person_source_value,
    mpi.*
  from mpi_final_base as mpi
  left join lth_bronze.src_flex__person as p
    on mpi.flex_patient_id = p.person_source_value
),

collapsed_nhs as (
  select distinct
    nhs_number,
    person_source_value,
    person_id,
    row_number() over (partition by nhs_number order by person_source_value)
      as id
  from mpi_final_all
  where
    person_source_value = flex_patient_id
    and nhs_number is not null
)

select
  coalesce(cn.person_id, mpi.person_id) as person_id,
  coalesce(cn.person_source_value, mpi.person_source_value)
    as person_source_value,
  coalesce(cn.nhs_number, mpi.nhs_number) as nhs_number,
  flex_patient_id,
  flex_mrn,
  scr_patient_id,
  birth_datetime,
  death_datetime,
  last_edit_time,
  source,
  nhs_number_count,
  flex_patient_id_count,
  flex_mrn_count,
  scr_patient_id_count,
  row_number() over (partition by coalesce(cn.person_id, mpi.person_id) order by last_edit_time desc) as current_record,
  @generate_surrogate_key(flex_patient_id, flex_mrn, coalesce(scr_patient_id, 0), coalesce(cn.nhs_number, mpi.nhs_number)) as unique_key,
  source_system,
  org_code
from mpi_final_all as mpi
left join collapsed_nhs as cn
  on
    mpi.nhs_number = cn.nhs_number
    and id = 1
