{{
  config(
    materialized = "table",
    tags = ['scr', 'staging', 'measurement']
    )
}}


SELECT
  care_id,
  mrn,
  nhs_number,
  cancer_site,
  icd_code,
  CAST(snomed_diagnosis AS VARCHAR(50)) AS snomed_diagnosis,
  diagnosis_date,
  diagnosis_basis,
  stage_type AS field,
  SUBSTRING(stage_value, 2, LEN(stage_value)) AS value,
  last_edit_time,
  updated_at
FROM
  (
    SELECT
      care_id,
      mrn,
      nhs_number,
      cancer_site,
      icd_code,
      snomed_diagnosis,
      diagnosis_date,
      diagnosis_basis,
      t_stage_pre_treatment AS cT,
      n_stage_pre_treatment AS cN,
      m_stage_pre_treatment AS cM,
      t_stage_final AS pT,
      n_stage_final AS pN,
      m_stage_final AS pM,
      last_edit_time,
      updated_at
    FROM
      lth_bronze.src_scr__cosd 
  ) AS staging_source
UNPIVOT
(
  stage_value FOR stage_type IN (
    ct,
    cn,
    cm,
    pt,
    pn,
    pm
  )
) AS staging_unpivoted

UNION

SELECT
  care_id,
  mrn,
  nhs_number,
  null AS cancer_site,
  diagnosis_site_icd,
  diagnosis_site_snomed,
  receipt_date,
  null AS basis,
  'Max Tumour Diameter' AS field,
  CAST(max_tumour_diameter_mm AS VARCHAR(50)) AS value,
  last_edit_time,
  updated_at
FROM
  lth_bronze.src_scr__pathology 
WHERE max_tumour_diameter_mm IS NOT NULL

UNION

SELECT
  care_id,
  mrn,
  nhs_number,
  cancer_site,
  icd_code,
  snomed,
  date,
  basis,
  marker_type AS field,   -- The name of the original column
  marker_value AS value,      -- The value from the original columns
  GETDATE() AS last_edit_time,
  GETDATE() AS updated_at
FROM
  (
    SELECT
      care_id,
      mrn,
      nhs_number,
      'Breast' AS cancer_site,
      null AS icd_code,
      null AS snomed,
      tumour_marker_date AS date,
      null AS basis,
      ps2 AS PS2,
      er AS ER,
      pr AS PR,
      c_erb_b2 AS [c-erbB-2],
      bcl_2 AS BCL2,
      her2 AS HER2,
      her2_fish AS [HER2 FISH]
    FROM
      lth_bronze.src_scr__breast_markers 
    WHERE
      tumour_marker_date IS NOT NULL
  ) AS markers_source
UNPIVOT
(
  marker_value FOR marker_type IN (
    PS2,
    ER,
    PR,
    [c-erbB-2],
    BCL2,
    HER2,
    [HER2 FISH]
  )
) AS unpivoted_markers

WHERE LEN(marker_value) >= 1
