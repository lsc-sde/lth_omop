
MODEL (
  name lth_bronze.src_scr__treatments,
  kind VIEW,
  cron '@daily',
);

select
  CARE_ID as care_id,
  [NHS Number] collate SQL_Latin1_General_CP1_CI_AS as nhs_number,
  [Hospital Number] collate SQL_Latin1_General_CP1_CI_AS as mrn,
  [Treatment Type] collate SQL_Latin1_General_CP1_CI_AS as treatment_type,
  [Treatment Event Type] collate SQL_Latin1_General_CP1_CI_AS as treatment_event_type,
  [Treatment Intent] collate SQL_Latin1_General_CP1_CI_AS as treatment_intent,
  [Adjunctive Therapy] collate SQL_Latin1_General_CP1_CI_AS as adjunctive_therapy,
  [Treatment Setting] collate SQL_Latin1_General_CP1_CI_AS as treatment_setting,
  Consultant collate SQL_Latin1_General_CP1_CI_AS as consultant,
  [Decision to treat date] as decision_to_treat_date,
  [Treatment start date] as treatment_start_date,
  [Organisation (Treatment) Site Code] collate SQL_Latin1_General_CP1_CI_AS as org_site_code_treatment,
  [Organisation (Treatment) Name] collate SQL_Latin1_General_CP1_CI_AS as org_treatment,
  [Discharge date] as discharge_date,
  [Waiting Time Adj DTT] as waiting_time_adj_dtt,
  [Validated for upload] collate SQL_Latin1_General_CP1_CI_AS as validated_upload,
  getdate() as last_edit_time,
  getdate() as updated_at,
  'rxn' collate SQL_Latin1_General_CP1_CI_AS as org_code,
  'scr' collate SQL_Latin1_General_CP1_CI_AS as source_system
from @scr_db.dbo.BIvwAllTreatments
