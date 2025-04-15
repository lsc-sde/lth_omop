
MODEL (
  name lth_bronze.src_scr__breast_markers,
  kind VIEW,
  cron '@daily',
);

select 
  CARE_ID AS care_id,
  [NHS Number] collate SQL_Latin1_General_CP1_CI_AS AS nhs_number,
  [Hospital Number] collate SQL_Latin1_General_CP1_CI_AS AS mrn,
  [Report Number] collate SQL_Latin1_General_CP1_CI_AS AS report_number,
  [Tumour Marker Date] AS tumour_marker_date,
  pS2 collate SQL_Latin1_General_CP1_CI_AS AS ps2,
  [pS2 Score] AS ps2_score,
  ER collate SQL_Latin1_General_CP1_CI_AS AS er,
  [ER Score] AS er_score,
  PR collate SQL_Latin1_General_CP1_CI_AS AS pr,
  [PR Score] AS pr_score,
  [C-erb B2] collate SQL_Latin1_General_CP1_CI_AS AS c_erb_b2,
  [C-erb B2 Score] AS c_erb_b2_score,
  [Bcl-2] collate SQL_Latin1_General_CP1_CI_AS AS bcl_2,
  [Bcl-2 Score] AS bcl_2_score,
  HER2 collate SQL_Latin1_General_CP1_CI_AS AS her2,
  [HER2 - FISH] AS her2_fish,
  'rxn' collate SQL_Latin1_General_CP1_CI_AS as org_code,
  'scr' collate SQL_Latin1_General_CP1_CI_AS as source_system
from @scr_db.dbo.BIvwBreastTumourMarkers
