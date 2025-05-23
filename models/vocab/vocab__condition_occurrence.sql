
MODEL (
  name lth_bronze.vocab__condition_occurrence,
  kind FULL,
  cron '@daily',
);

with concept as (
  select distinct
    concept_id,
    replace(concept_code, '.', '') as concept_code,
    invalid_reason
  from @catalog_src.@schema_vocab.CONCEPT
  where vocabulary_id in ('ICD10CM', 'SNOMED')
),

mappings as (
  select
    source.concept_code as icd_code,
    source.concept_name as icd_name,
    target.concept_id as snomed_code,
    target.concept_name as snomed_name
  from @catalog_src.@schema_vocab.concept as source
  inner join @catalog_src.@schema_vocab.concept_relationship as rel
    on
      source.concept_id = rel.concept_id_1
      and rel.invalid_reason is null
      and rel.relationship_id = 'Maps to'
  inner join @catalog_src.@schema_vocab.concept as target
    on
      rel.concept_id_2 = target.concept_id
      and target.invalid_reason is null
  where
    source.vocabulary_id = 'ICD10CM'
    and target.vocabulary_id = 'SNOMED'
),

snomed_mappings as (
  select
    source.concept_id as snomed_code,
    source.concept_code as concept_snomed,
    source.concept_name as snomed_name,
    target.concept_id as new_snomed_code,
    target.concept_name as new_snomed_name
  from @catalog_src.@schema_vocab.concept as source
  inner join @catalog_src.@schema_vocab.concept_relationship as rel
    on
      source.concept_id = rel.concept_id_1
      and rel.invalid_reason is null
      and rel.relationship_id = 'Maps to'
  inner join @catalog_src.@schema_vocab.concept as target
    on
      rel.concept_id_2 = target.concept_id
      and target.invalid_reason is null
),

condition as (
  select
    person_id,
    isnull(mp.snomed_code, isnull(sp.new_snomed_code, cm.concept_id))
      as condition_concept_id,
    episode_start_dt as condition_start_date,
    episode_end_dt as condition_end_date,
    case when source_system = 'scr' then 32879 else 32817 end
      as condition_type_concept_id,
    case
      when source_system = 'scr' then 32902
      when episode_coding_position = 1 then 32903
      else 32909
    end as condition_status_concept_id,
    provider_id,
    isnumeric(provider_id) as provider_id_type,
    visit_occurrence_id,
    co.source_code as condition_source_value,
    cm.concept_id as condition_source_concept_id,
    org_code,
	  source_system,
	  last_edit_time,
	  updated_at
  from lth_bronze.stg__condition_occurrence as co
  inner join concept as cm
    on replace(co.source_code, '.', '') = cm.concept_code
  left join mappings as mp
    on
      replace(co.source_code, '.', '') = replace(mp.icd_code, '.', '')
      and co.source_system in ('ukcoder')
  left join snomed_mappings as sp
    on
      co.source_code = sp.concept_snomed
      and co.source_system = 'flex_ae'
)

select c.*
from condition as c
inner join @catalog_src.@schema_vocab.CONCEPT as cn
  on c.condition_concept_id = cn.concept_id
where cn.domain_id = 'Condition'
