
MODEL (
  name lth_bronze.src_scr__anti_cancer_drugs,
  kind VIEW,
  cron '@daily',
);

select
  [CARE_ID] as care_id,
  [Consultant] collate SQL_Latin1_General_CP1_CI_AS as consultant,
  [Cancer Site] collate SQL_Latin1_General_CP1_CI_AS as cancer_site,
  [NHS Number] collate SQL_Latin1_General_CP1_CI_AS as nhs_number,
  [Hospital Number] collate SQL_Latin1_General_CP1_CI_AS as mrn,
  [Date Decision to Treat] as date_decision_to_treat,
  [Organisation (DTT)] collate SQL_Latin1_General_CP1_CI_AS as organisation,
  [Date Start of Treatment] as date_treatment_start,
  [Organisation (Treatment) Site Code] collate SQL_Latin1_General_CP1_CI_AS as treatment_site_code,
  [Organisation (Treatment) Name] collate SQL_Latin1_General_CP1_CI_AS as organisation_name,
  [Planned Cycles/Courses] collate SQL_Latin1_General_CP1_CI_AS as planned_cycles,
  [Chemo-Radiotherapy] collate SQL_Latin1_General_CP1_CI_AS as chemo_radiotherapy,
  [Treatment Event Type] collate SQL_Latin1_General_CP1_CI_AS as treatment_event_type,
  [Treatment Setting] collate SQL_Latin1_General_CP1_CI_AS as treatment_setting,
  [Clinical Trial] collate SQL_Latin1_General_CP1_CI_AS as clinical_trial,
  [Drug Therapy Type] collate SQL_Latin1_General_CP1_CI_AS as drug_therapy_type,
  [Treatment Intent] collate SQL_Latin1_General_CP1_CI_AS as treatment_intent,
  [Adjunctive Therapy] collate SQL_Latin1_General_CP1_CI_AS as adjunctive_therapy,
  [Route of Administration] collate SQL_Latin1_General_CP1_CI_AS as route_of_administration,
  [Drug Regimen Acronym] collate SQL_Latin1_General_CP1_CI_AS as drug_regiment_acronym,
  [TACE Performed Indicator] collate SQL_Latin1_General_CP1_CI_AS as tace_performed_indicator,
  [Chemotherapy Consultant Age Specialty] collate SQL_Latin1_General_CP1_CI_AS as chemo_cons_age_spec,
  [Review/End Date] as rev_end_date,
  getdate() as last_edit_time,
  getdate() as updated_at,
  'rxn' collate SQL_Latin1_General_CP1_CI_AS as org_code,
  'scr' collate SQL_Latin1_General_CP1_CI_AS as source_system
from @scr_db.dbo.BIvwTreatmentAntiCancerDrugs
