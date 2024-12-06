
MODEL (
  name lth_bronze.stg_scr__result,
  kind FULL,
  cron '@daily',
);


select
  care_id,
  mrn,
  nhs_number,
  cancer_site,
  icd_code,
  cast(snomed_diagnosis as VARCHAR(50)) as snomed_diagnosis,
  diagnosis_date,
  diagnosis_basis,
  stage_type as field,
  SUBSTRING(stage_value, 2, LEN(stage_value)) as value,
  last_edit_time,
  updated_at
from
  (
    select
      care_id,
      mrn,
      nhs_number,
      cancer_site,
      icd_code,
      snomed_diagnosis,
      diagnosis_date,
      diagnosis_basis,
      t_stage_pre_treatment as cT,
      n_stage_pre_treatment as cN,
      m_stage_pre_treatment as cM,
      t_stage_final as pT,
      n_stage_final as pN,
      m_stage_final as pM,
      last_edit_time,
      updated_at
    from
      lth_bronze.src_scr__cosd 
  ) as staging_source
unpivot
(
  stage_value for stage_type in (
    ct,
    cn,
    cm,
    pt,
    pn,
    pm
  )
) as staging_unpivoted

union

select
  care_id,
  mrn,
  nhs_number,
  null as cancer_site,
  diagnosis_site_icd,
  diagnosis_site_snomed,
  receipt_date,
  null as basis,
  'Max Tumour Diameter' as field,
  cast(max_tumour_diameter_mm as VARCHAR(50)) as value,
  last_edit_time,
  updated_at
from
  lth_bronze.src_scr__pathology 
where max_tumour_diameter_mm is not null

union

select
  care_id,
  mrn,
  nhs_number,
  cancer_site,
  icd_code,
  snomed,
  date,
  basis,
  marker_type as field,   -- The name of the original column
  marker_value as value,      -- The value from the original columns
  GETDATE() as last_edit_time,
  GETDATE() as updated_at
from
  (
    select
      care_id,
      mrn,
      nhs_number,
      'Breast' as cancer_site,
      null as icd_code,
      null as snomed,
      tumour_marker_date as date,
      null as basis,
      ps2 as PS2,
      er as ER,
      pr as PR,
      c_erb_b2 as [c-erbB-2],
      bcl_2 as BCL2,
      her2 as HER2,
      her2_fish as [HER2 FISH]
    from
      lth_bronze.src_scr__breast_markers 
    where
      tumour_marker_date is not null
  ) as markers_source
unpivot
(
  marker_value for marker_type in (
    PS2,
    ER,
    PR,
    [c-erbB-2],
    BCL2,
    HER2,
    [HER2 FISH]
  )
) as unpivoted_markers

where LEN(marker_value) >= 1

union

select
  care_id,
  mrn,
  nhs_number,
  null as cance_site,
  null as icd_code,
  null as snomed_diagnosis,
  assessment_date,
  null as basis,
  a_type,
  a_value,
  GETDATE() as last_edit_time,
  GETDATE() as updated_at
from
  (
    select
      care_id,
      nhs_number,
      mrn,
      assessment_date,
      cast(menstrual_status as VARCHAR) as [Menstrual Status],
      cast(smoking_status as VARCHAR) as [Smoking Status]
    from lth_bronze.src_scr__initial_assessment 
  ) as assessment_source
unpivot
(
  a_value for a_type in (
    [Menstrual Status],
    [Smoking Status]
  )
) as assessment_source

union

select
  care_id,
  mrn,
  nhs_number,
  cancer_site,
  icd_primary_diagnosis,
  snomed_primary_diagnosis,
  date_of_diagnosis,
  basis_of_diagnosis,
  'Grade of Differentiation' as field,
  grade_of_differentiation as value,
  last_edit_time,
  updated_at
from lth_bronze.src_scr__diagnosis 
where grade_of_differentiation is not null

union

select
  care_id,
  mrn,
  nhs_number,
  cancer_site,
  null as icd,
  null as snomed,
  start_date,
  null as basis,
  'Trial Status' as field,
  clinical_trial_status as value,
  GETDATE() as last_edit_time,
  GETDATE() as updated_at
from lth_bronze.src_scr__trials 
where
  clinical_trial_status = 'Patient eligible, consented to and entered trial'
  and start_date is not null
