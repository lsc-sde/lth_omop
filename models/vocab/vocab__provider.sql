
MODEL (
  name lth_bronze.vocab__provider,
  kind FULL,
  cron '@daily',
);
select
  provider_name,
  care_site_id,
  provider_source_value,
  provider_id,
  specialty_source_value,
  vm.source_code_description,
  vm.target_concept_id,
  cons_org_code
from lth_bronze.stg__provider as p
left join
  (
    select distinct
      source_code_description,
      target_concept_id
    from lth_bronze.vocab__source_to_concept_map 
    where [group] = 'specialty'
  ) as vm
  on p.specialty_source_value = vm.source_code_description
