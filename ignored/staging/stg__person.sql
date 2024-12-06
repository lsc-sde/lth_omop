
MODEL (
  name lth_bronze.stg__person,
  kind FULL,
  cron '@daily',
);

with person as (
    select distinct
    mpi.person_id,
    mpi.person_source_value,
    mpi.nhs_number,
    mpi.source,
    first_value(isnull(fp.gender_source_value, case when sex = 1 then 'Male' when sex = 2 then 'Female' else 'Unknown' end)) over (partition by mpi.person_id order by case when isnull(fp.gender_source_value, case when sex = 1 then 'Male' when sex = 2 then 'Female' else 'Unknown' end) is not null then 1 else 2 end, fp.last_edit_time desc) as gender_source_value,
    first_value(isnull(cast(fp.race_source_value as varchar), ethnicity)) over (partition by mpi.person_id order by case when isnull(cast(fp.race_source_value as varchar), ethnicity) is not null then 1 else 2 end, fp.last_edit_time desc) race_source_value,
    first_value(isnull(fp.mailing_code, postcode)) over (partition by mpi.person_id order by case when isnull(fp.mailing_code, postcode) is not null then 1 else 2 end, fp.last_edit_time desc) as mailing_code,
    first_value(isnull(cast(fp.provider_id as varchar), sp.gp_code)) over (partition by mpi.person_id order by case when isnull(cast(fp.provider_id as varchar), sp.gp_code) is not null then 1 else 2 end, fp.last_edit_time desc) as provider_id,
    first_value(isnull(fp.gp_prac_code, sp.gp_practice)) over (partition by mpi.person_id order by case when isnull(fp.gp_prac_code, sp.gp_practice) is not null then 1 else 2 end, fp.last_edit_time desc) as gp_prac_code,
    first_value(isnull(fp.birth_datetime, sp.date_of_birth)) over (partition by mpi.person_id order by case when isnull(fp.birth_datetime, sp.date_of_birth) is not null then 1 else 2 end, fp.last_edit_time desc) as birth_datetime,
    first_value(isnull(fp.death_datetime, sp.date_of_death)) over (partition by mpi.person_id order by case when isnull(fp.death_datetime, sp.date_of_death) is not null then 1 else 2 end, fp.last_edit_time desc) as death_datetime,
	  first_value(fp.mother_patient_id) over (partition by mpi.person_id order by case when fp.mother_patient_id is not null then 1 else 2 end, fp.last_edit_time desc) as mother_person_source_value,
	  isnull(first_value(fp.last_edit_time) over (partition by mpi.person_id order by fp.last_edit_time desc), getdate()) as last_edit_time
  from lth_bronze.stg__master_patient_index as mpi
  left join lth_bronze.src_flex__person as fp
    on mpi.flex_patient_id = fp.person_source_value and mpi.source = 'flex'
  left join lth_bronze.src_scr__person as sp
    on mpi.nhs_number = sp.nhs_number and mpi.source = 'scr'
  where
    collapsed_into_patient_id is null
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
    person.source,
    person.last_edit_time
  from
    person