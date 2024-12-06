{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'pathology']
    )
}}

select
  care_id,
  cancer_site,
  nhs_number,
  mrn,
  receipt_date,
  reporting_date,
  report_number,
  investigation_type,
  nature_of_specimen,
  report_status,
  surgeon,
  requesting_organisation,
  pathologist,
  reporting_organisation,
  diagnosis_site_icd,
  diagnosis_site_snomed,
  laterality,
  max_tumour_diameter_mm,
  num_nodes_examined,
  num_pos_nodes,
  type_snomed,
  grade_differentiation,
  excision_margins,
  vascular_lymphatic_invasion,
  synchronous_tumour,
  pathological_staging,
  comments,
  last_edit_time,
  updated_at
from {{ source('omop_source', 'src_scr__pathology') }}
