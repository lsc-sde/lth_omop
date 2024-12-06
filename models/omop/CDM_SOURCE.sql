{{
  config(
    materialized = "table"
    )
}}

select
  'LTH OMOP Bronze' as cdm_source_name,
  'LTH_OMOP_Bronze' as cdm_source_abbreviation,
  'Lancashire Teaching Hospitals NHS Foundation Trust' as cdm_holder,
  'Multi-source secondary care dataset' as source_description,
  'http://omop-lsc.surge.sh/' as source_documentation_reference,
  'http://omop-lsc.surge.sh/' as cdm_etl_reference,
  cast(getdate() as date) as source_release_date,
  cast(getdate() as date) as cdm_release_date,
  '5.4' as cdm_version,
  '756265' as cdm_version_concept_id,
  'v5.0 09-SEP-22' as vocabulary_version
