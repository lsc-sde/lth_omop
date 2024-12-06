{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'treatments']
    )
}}

select
  care_id,
  nhs_number,
  mrn,
  treatment_type,
  treatment_event_type,
  treatment_intent,
  adjunctive_therapy,
  treatment_setting,
  consultant,
  decision_to_treat_date,
  treatment_start_date,
  org_site_code_treatment,
  org_treatment,
  discharge_date,
  waiting_time_adj_dtt,
  validated_upload,
  last_edit_time,
  updated_at
from @catalaog_src.@schema_src.src_scr__treatments
