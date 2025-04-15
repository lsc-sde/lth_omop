
MODEL (
  name lth_bronze.src_scr__trials,
  kind VIEW,
  cron '@daily',
);

select
  CARE_ID as care_id,
  [Cancer Site] collate SQL_Latin1_General_CP1_CI_AS as cancer_site,
  [NHS Number] collate SQL_Latin1_General_CP1_CI_AS as nhs_number,
  [Hospital Number] collate SQL_Latin1_General_CP1_CI_AS as mrn,
  [Clinical Trial Status] collate SQL_Latin1_General_CP1_CI_AS as clinical_trial_status,
  [Consent Date] as consent_date,
  [Trial Name] collate SQL_Latin1_General_CP1_CI_AS as trial_name,
  [Trial Type] collate SQL_Latin1_General_CP1_CI_AS as trial_type,
  [Local/National Trial] collate SQL_Latin1_General_CP1_CI_AS as local_national_trial,
  [Trial Number] collate SQL_Latin1_General_CP1_CI_AS as trial_number,
  Regimen collate SQL_Latin1_General_CP1_CI_AS as regimen,
  [Randomised Date] as randomised_date,
  [Start Date] as start_date,
  Consultant collate SQL_Latin1_General_CP1_CI_AS as consultant,
  Investigator collate SQL_Latin1_General_CP1_CI_AS as investigator,
  getdate() as last_edit_time,
  getdate() as updated_at,
  'rxn' collate SQL_Latin1_General_CP1_CI_AS as org_code,
  'scr' collate SQL_Latin1_General_CP1_CI_AS as source_system
from @scr_db.dbo.BIvwTreatmentClinicalTrial
