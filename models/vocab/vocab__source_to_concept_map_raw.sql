
MODEL (
  name lth_bronze.vocab__source_to_concept_map_raw,
  kind FULL,
  cron '@daily',
);

select
  source_code::VARCHAR(MAX),
  source_code_description::VARCHAR(MAX),
  target_concept_id::INT,
  target_concept_name::VARCHAR(MAX),
  target_domain_id::VARCHAR(50),
  "group"::VARCHAR(50),
  source::VARCHAR(50),
  frequency::INT,
  mapping_status::VARCHAR(50)
from (

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)) as source_code_description,
    conceptId as target_concept_id,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'drugs' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__drugs_mappings

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'job_role' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__job_role

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'specialty' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__medical_specialty

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'radiology' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__radiology_mappings

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'result' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__vital_signs_mappings

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'demographics' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__patient_demographics

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'drug_routes' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__drug_routes

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'result' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__blood_mappings

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'units' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__unit_mappings
  where sourceCode not in ('RD/3698/13',
                          'RD/3891/13',
                          'RD/1962/14',
                          'RD/3795/14',
                          'RD/3659/34',
                          'RD/3698/11',
                          'RD/3698/10',
                          'RD/4899/13',
                          'RD/3585/5',
                          'RD/3585/10',
                          'RD/4555/5',
                          'RD/3626/9',
                          'RD/4710/5',
                          'RD/1962/19',
                          'RD/3673/5',
                          'RD/4699/42')

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'units' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__unit_mappings_bloods

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'decoded' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__decoded

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'discharge_destination' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__discharge_destination

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'admission_source' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__admission_source

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'specimen_type' as "group",
    'swisslab' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.swisslab__site_mappings

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'anatomical_site' as "group",
    'swisslab' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.swisslab__anatomical_site_mappings

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'result' as "group",
    'bi_referrals' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__referrals

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'referral_priority' as "group",
    'bi_referrals' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__referral_priority

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'bacteria_presence' as "group",
    'swisslab' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.swisslab__bacteria_presence

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'bacteria_observation' as "group",
    'swisslab' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.swisslab__bacteria_observation

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'bacteria_sensitivities' as "group",
    'swisslab' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.swisslab__antibiotic_sensitivities

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'bacteriology_other_test' as "group",
    'swisslab' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.swisslab__misc_test

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'bacteriology_other_result' as "group",
    'swisslab' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.swisslab__misc_test_result

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'devices' as "group",
    'flex' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.flex__devices

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'scr_results' as "group",
    'scr' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.scr__results

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'scr_fields' as "group",
    'scr' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.scr__field_mappings

  union

  select
    cast(sourceCode as varchar) as source_code,
    cast(sourceName as varchar(450)),
    conceptId,
    conceptName as target_concept_name,
    domainId as target_domain_id,
    'scr_conditions' as "group",
    'scr' as source,
    sourceFrequency as frequency,
    mappingStatus as mapping_status
  from lth_bronze.scr__conditions_other

) as cm

--where mapping_status = 'APPROVED'
