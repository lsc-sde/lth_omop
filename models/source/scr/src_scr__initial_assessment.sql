
MODEL (
  name lth_bronze.src_scr__initial_assessment,
  kind VIEW,
  cron '@daily',
);

select 
  ia.CARE_ID AS care_id,
  d.N1_1_NHS_NUMBER collate SQL_Latin1_General_CP1_CI_AS AS nhs_number,
  d.N1_2_HOSPITAL_NUMBER collate SQL_Latin1_General_CP1_CI_AS AS mrn,
  ia.L_ASSESS_DATE AS assessment_date,
  CASE
    WHEN ia.N_B4_MENSTRUAL_STATUS = 1 THEN 'Premenopausal'
    WHEN ia.N_B4_MENSTRUAL_STATUS = 2 THEN 'Perimenopausal'
    WHEN ia.N_B4_MENSTRUAL_STATUS = 3 THEN 'Postmenopausal'
    WHEN ia.N_B4_MENSTRUAL_STATUS = 9 THEN 'Not Known'
  END collate SQL_Latin1_General_CP1_CI_AS AS menstrual_status,
  ia.L_AGE_MENO AS age_menopause,
  CASE
    WHEN ia.L_MEDICAL_MEN = 1 THEN 'Yes'
    WHEN ia.L_MEDICAL_MEN = 2 THEN 'No'
  END collate SQL_Latin1_General_CP1_CI_AS AS medical_menopause,
  CASE
    WHEN ia.N_HN5_SMOKER = 1 THEN 'Current'
    WHEN ia.N_HN5_SMOKER = 2 THEN 'Ex Smoker'
    WHEN ia.N_HN5_SMOKER = 3 THEN 'Non Smoker'
    WHEN ia.N_HN5_SMOKER = 4 THEN 'Never Smoked'
    WHEN ia.N_HN5_SMOKER = 9 THEN 'Unknown'
  END collate SQL_Latin1_General_CP1_CI_AS AS smoking_status,
  'rxn' collate SQL_Latin1_General_CP1_CI_AS AS org_code,
  'scr' collate SQL_Latin1_General_CP1_CI_AS AS source_system
FROM @scr_db.dbo.tblMAIN_REFERRALS AS mr
INNER JOIN @scr_db.dbo.tblINITIAL_ASSESSMENT AS ia
  ON mr.CARE_ID = ia.CARE_ID
LEFT JOIN @scr_db.dbo.tblDEMOGRAPHICS AS d
  ON mr.PATIENT_ID = d.PATIENT_ID
