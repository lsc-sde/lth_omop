
MODEL (
  name lth_bronze.stg__person,
  kind FULL,
  cron '@daily',
);

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('PERSON') and datasource = 'flex'
),

person as (
    select distinct
    mpi.person_id,
    mpi.person_source_value,
    mpi.nhs_number,
    mpi.source,
    
    first_value(fp.gender_source_value) over (partition by mpi.person_id order by case when fp.gender_source_value is not null then 1 else 2 end, fp.last_edit_time desc) as gender_source_value,
    FIRST_VALUE(CAST(fp.race_source_value AS VARCHAR)) OVER (PARTITION BY mpi.person_id ORDER BY CASE WHEN fp.race_source_value IS NOT NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC) AS race_source_value,
    FIRST_VALUE(fp.mailing_code) OVER (PARTITION BY mpi.person_id ORDER BY CASE WHEN fp.mailing_code IS NOT NULL THEN 1 ELSE 2 END, fp.last_edit_time DESC) AS mailing_code,
    first_value(cast(fp.provider_id as varchar)) over (partition by mpi.person_id order by case when cast(fp.provider_id as varchar) is not null then 1 else 2 end, fp.last_edit_time desc) as provider_id,
    first_value(fp.gp_prac_code) over (partition by mpi.person_id order by case when fp.gp_prac_code is not null then 1 else 2 end, fp.last_edit_time desc) as gp_prac_code,
    first_value(fp.birth_datetime) over (partition by mpi.person_id order by case when fp.birth_datetime is not null then 1 else 2 end, fp.last_edit_time desc) as birth_datetime,
	  first_value(fp.death_datetime) over (partition by mpi.person_id order by case when fp.death_datetime is not null then 1 else 2 end, fp.last_edit_time desc) as death_datetime,
    first_value(fp.mother_patient_id) over (partition by mpi.person_id order by case when fp.mother_patient_id is not null then 1 else 2 end, fp.last_edit_time desc) as mother_person_source_value,
    isnull(first_value(fp.last_edit_time) over (partition by mpi.person_id order by fp.last_edit_time desc), getdate()) as last_edit_time,
    mpi.source_system,
    mpi.org_code
  from lth_bronze.stg__master_patient_index as mpi
  left join lth_bronze.src_flex__person as fp
    on mpi.flex_patient_id = fp.person_source_value and mpi.source = 'flex'
  where
    collapsed_into_patient_id is null
  and mpi.last_edit_time >=  (
      select updated_at from cdc
    )
  and mpi.last_edit_time <= getdate()
)

  select
    person.person_id,
    person.person_source_value,
    person.nhs_number,
    person.provider_id,
    person.gp_prac_code,
    person.mother_person_source_value,
    person.birth_datetime,
    person.death_datetime,
    person.gender_source_value,
    person.race_source_value,
    person.mailing_code,
    person.source_system,
    person.org_code,
    person.last_edit_time
  from
    person