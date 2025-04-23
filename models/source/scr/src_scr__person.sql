
MODEL (
  name lth_bronze.src_scr__person,
  kind VIEW,
  cron '@daily',
);

with Merged as (

  select
    PATIENT_ID as patient_id,
    N1_1_NHS_NUMBER as nhs_number,
    N1_2_HOSPITAL_NUMBER as mrn,
    min(PATIENT_ID) over (partition by N1_1_NHS_NUMBER) as first_patient_id
  from @scr_db.dbo.tblDEMOGRAPHICS
  group by
    PATIENT_ID,
    N1_1_NHS_NUMBER,
    N1_2_HOSPITAL_NUMBER
)

select distinct
  m.first_patient_id as person_source_value,
  d.N1_1_NHS_NUMBER collate SQL_Latin1_General_CP1_CI_AS as nhs_number,
  d.N1_2_HOSPITAL_NUMBER collate SQL_Latin1_General_CP1_CI_AS as mrn,
  dm.N1_8_POSTCODE collate SQL_Latin1_General_CP1_CI_AS as postcode,
  dm.N1_9_SEX collate SQL_Latin1_General_CP1_CI_AS as sex,
  dm.N1_10_DATE_BIRTH as date_of_birth,
  dm.N1_11_GP_CODE collate SQL_Latin1_General_CP1_CI_AS as gp_code,
  dm.N1_12_GP_PRACTICE_CODE collate SQL_Latin1_General_CP1_CI_AS as gp_practice,
  dm.N15_1_DATE_DEATH as date_of_death,
  case
    when len(dm.N1_15_ETHNICITY) = 0 then null else dm.N1_15_ETHNICITY
  end collate SQL_Latin1_General_CP1_CI_AS as ethnicity,
  'rxn' collate SQL_Latin1_General_CP1_CI_AS as org_code,
  'scr' collate SQL_Latin1_General_CP1_CI_AS as source_system
from @scr_db.dbo.tblDEMOGRAPHICS as d
inner join Merged as m
  on d.PATIENT_ID = m.patient_id
left join @scr_db.dbo.tblDEMOGRAPHICS as dm
  on m.first_patient_id = dm.PATIENT_ID
