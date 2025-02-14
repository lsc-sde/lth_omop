
MODEL (
  name lth_bronze.vocab__procedure_occurrence,
  kind FULL,
  cron '@daily',
);

with concept as (
  select
    concept_id,
    concept_code,
    concept_name
  from dbt_omop.vocab.CONCEPT
  where vocabulary_id in ('OPCS4', 'SNOMED')
  union
  select
    target_concept_id,
    source_code,
    target_concept_name
  from lth_bronze.vocab__source_to_concept_map
  where concept_group = 'radiology'
),

procs as (
  select
    po.person_id,
    po.visit_occurrence_id,
    po.procedure_date,
    po.procedure_datetime,
    po.provider_id,
	provider_id_type,
    isnull(po.procedure_source_value, cm.concept_name)
      as procedure_source_value,
    32817 as procedure_type_concept_id,
    po.source_code,
    cm.concept_id,
    last_edit_time,
	org_code,
    source_system
  from lth_bronze.stg__procedure_occurrence as po
  inner join concept as cm
    on po.source_code = cm.concept_code
),

prov as (
  select
	  cons_org_code,
	  provider_id
  from lth_bronze.vocab__provider
  where cons_org_code is not null
),

prov1 as (
  select
	  provider_id,
	  provider_source_value
  from lth_bronze.vocab__provider
)

select
  p.person_id,
  p.visit_occurrence_id,
  p.procedure_date,
  p.procedure_datetime,
  coalesce(pr.provider_id, pr1.provider_id) as provider_id,
  p.procedure_source_value,
  p.procedure_type_concept_id,
  p.source_code,
  p.concept_id,
  last_edit_time,
  p.org_code,
  p.source_system
from procs as p
inner join dbt_omop.vocab.CONCEPT as cn
  on p.concept_id = cn.concept_id
left join prov pr
  on pr.cons_org_code = p.provider_id
  and provider_id_type = 0
left join prov1 pr1
  on pr1.provider_source_value = p.provider_id
  and provider_id_type = 1
where
  cn.domain_id = 'Procedure'
  and cn.invalid_reason is null;