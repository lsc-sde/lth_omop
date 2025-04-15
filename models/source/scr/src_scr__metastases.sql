
MODEL (
  name lth_bronze.src_scr__metastases,
  kind VIEW,
  cron '@daily',
);

select
  CareID AS care_id,
  [Cancer Site] collate SQL_Latin1_General_CP1_CI_AS AS cancer_site,
  Location collate SQL_Latin1_General_CP1_CI_AS AS location,
  Certainty collate SQL_Latin1_General_CP1_CI_AS AS certainty,
  Type collate SQL_Latin1_General_CP1_CI_AS AS type,
  [Other Mets Details] collate SQL_Latin1_General_CP1_CI_AS AS other_mets_details,
  'rxn' collate SQL_Latin1_General_CP1_CI_AS as org_code,
  'scr' collate SQL_Latin1_General_CP1_CI_AS as source_system
from @scr_db.dbo.BIvwMetastases
