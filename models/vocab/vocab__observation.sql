
MODEL (
  name lth_bronze.vocab__observation,
  kind view,
  cron '@daily',
);

with concept as (
  select distinct
    concept_id,
    concept_code
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

cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at
  where
    model in ('OBSERVATION') and datasource = 'ukcoder'
),

condition as (
  select
    person_id,
    isnull(mp.snomed_code, cm.concept_id) as condition_concept_id,
    episode_start_dt as condition_start_date,
    episode_end_dt as condition_end_date,
    32817 as condition_type_concept_id,
    case
      when episode_coding_position = 1 then 32903 else 32909
    end as condition_status_concept_id,
    provider_id,
    visit_occurrence_id,
    co.source_code as condition_source_value,
    cm.concept_id as condition_source_concept_id,
    co.org_code,
    co.source_system,
    co.last_edit_time,
    co.updated_at
  from lth_bronze.stg__condition_occurrence as co
  inner join concept as cm
    on co.source_code = cm.concept_code
  left join mappings as mp
    on co.source_code = mp.icd_code
  where
    co.updated_at > (
      select updated_at from cdc
    )
    and co.updated_at <= getdate()
)

select distinct
  r.person_id,
  r.visit_occurrence_id,
  null as visit_detail_id,
  r.measurement_event_id as observation_event_id,
  cast(r.provider_id as varchar) as provider_id,
  r.result_datetime,
  cm.target_concept_id,
  32817 as type_concept_id,
  r.source_value as value_as_number,
  r.value_source_value as value_as_string,
  coalesce(cm_dc.target_concept_id, cm_rp.target_concept_id)
    as value_as_concept_id,
  null as qualifier_concept_id,
  null as unit_concept_id,
  r.source_name as source_value,
  null as observation_source_concept_id,
  null as unit_source_value,
  cast(null as varchar(50)) as qualifier_source_value,
  r.value_source_value,
  null as obs_event_field_concept_id,
  r.org_code,
  r.source_system,
  r.updated_at as last_edit_time
from lth_bronze.stg__result as r
inner join
  (
    select
      source_code,
      target_concept_id,
      target_domain_id
    from lth_bronze.vocab__source_to_concept_map
    where
      concept_group = 'result'
      or (concept_group = 'bacteria_observation' and source_system = 'swisslab')
  ) as cm
  on r.source_code = cm.source_code
left join
  (
    select distinct
      source_code,
      target_concept_id,
      target_domain_id
    from lth_bronze.vocab__source_to_concept_map
    where
      concept_group in ('referral_priority')
  ) as cm_rp
  on r.priority = cm_rp.source_code
left join
  (
    select distinct
      source_code,
      source_code_description,
      target_concept_id,
      target_domain_id
    from lth_bronze.vocab__source_to_concept_map
    where
      concept_group in ('decoded')
  ) as cm_dc
  on
    r.source_code = cm_dc.source_code
    and r.value_source_value = cm_dc.source_code_description
where
  cm.target_domain_id = 'Observation'

union

select distinct
  c.person_id,
  c.visit_occurrence_id,
  null as visit_detail_id,
  cast(null as varchar) as observation_event_id,
  provider_id,
  condition_start_date,
  condition_concept_id,
  32817 as type_concept_id,
  null as value_as_number,
  cn.concept_name as value_as_string,
  null as value_as_concept_id,
  null as qualifier_concept_id,
  null as unit_concept_id,
  c.condition_source_value as source_value,
  c.condition_source_concept_id as observation_source_concept_id,
  null as unit_source_value,
  cast(null as varchar(50)) as qualifier_source_value,
  c.condition_source_value as value_source_value,
  null as obs_event_field_concept_id,
  org_code,
  source_system,
  c.updated_at
from condition as c
inner join @catalog_src.@schema_vocab.CONCEPT as cn
  on c.condition_concept_id = cn.concept_id
where cn.domain_id = 'Observation'

union

select distinct
  c.person_id,
  c.visit_occurrence_id,
  null as visit_detail_id,
  cast(null as varchar) as observation_event_id,
  provider_id,
  condition_start_date,
  condition_concept_id,
  32817 as type_concept_id,
  null as value_as_number,
  cn.concept_name as value_as_string,
  null as value_as_concept_id,
  null as qualifier_concept_id,
  null as unit_concept_id,
  c.condition_source_value as source_value,
  c.condition_source_concept_id as observation_source_concept_id,
  null as unit_source_value,
  cast(null as varchar(50)) as qualifier_source_value,
  c.condition_source_value as value_source_value,
  null as obs_event_field_concept_id,
  org_code,
  source_system,
  c.updated_at
from condition as c
inner join @catalog_src.@schema_vocab.CONCEPT as cn
  on c.condition_concept_id = cn.concept_id
where cn.domain_id = 'Observation'

union

select distinct
  r.person_id,
  r.visit_occurrence_id,
  null as visit_detail_id,
  r.measurement_event_id as observation_event_id,
  cast(r.provider_id as varchar) as provider_id,
  r.result_datetime,
  c.target_concept_id,
  32817 as type_concept_id,
  r.source_value as value_as_number,
  r.value_source_value as value_as_string,
  c2.target_concept_id
    as value_as_concept_id,
  null as qualifier_concept_id,
  null as unit_concept_id,
  r.source_name as source_value,
  null as observation_source_concept_id,
  null as unit_source_value,
  cast(null as varchar(50)) as qualifier_source_value,
  r.value_source_value,
  null as obs_event_field_concept_id,
  r.org_code,
  r.source_system,
  r.updated_at
from lth_bronze.stg__result as r
left join
  (
    select 
      source_code,
      source_code_description,
      target_concept_id,
      target_domain_id
    from lth_bronze.vocab__source_to_concept_map
    where concept_group = 'scr_fields'
  ) as c
  on r.source_code = c.source_code
left join
  (
    select 
      source_code,
      source_code_description,
      target_concept_id,
      target_domain_id
    from lth_bronze.vocab__source_to_concept_map
    where concept_group = 'scr_results'
  ) as c2
  on
    r.value_source_value = c2.source_code_description
    and r.source_name = c2.source_code
left join @catalog_src.@schema_vocab.CONCEPT as cn
  on c.target_concept_id = cn.concept_id
where
  source_system = 'scr'
  and cn.domain_id = 'Observation'

union

select distinct
  person_id,
  null as visit_occurrence_id,
  null as visit_detail_id,
  cast(null as varchar) as observation_event_id,
  null as provider_id,
  insert_datetime,
  44787910 as concept_id,
  32848 as type_concept_id,
  null as value_as_number,
  'Informed dissent for national audit' as value_as_string,
  null as value_as_concept_id,
  null as qualifier_concept_id,
  null as unit_concept_id,
  'Informed dissent for national audit' as source_value,
  null as observation_source_concept_id,
  null as unit_source_value,
  cast(null as varchar(50)) as qualifier_source_value,
  'Informed dissent for national audit' as value_source_value,
  null as obs_event_field_concept_id,
  'rxn' as org_code,
  'mesh' as source_system,
  insert_datetime as updated_at
from lth_bronze.ext__data_opt_out
where person_id is not null