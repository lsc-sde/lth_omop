
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
  from vocab.CONCEPT
  where vocabulary_id in ('OPCS4', 'SNOMED')
  union
  select
    target_concept_id,
    source_code,
    target_concept_name
  from lth_bronze.vocab__source_to_concept_map 
  where [group] = 'radiology'
),

procs as (
  select
    po.person_id,
    po.visit_occurrence_id,
    po.procedure_date,
    po.procedure_datetime,
    po.provider_id,
    isnull(po.procedure_source_value, cm.concept_name)
      as procedure_source_value,
    32817 as procedure_type_concept_id,
    po.source_code,
    cm.concept_id,
    last_edit_time,
    data_source
  from lth_bronze.stg__procedure_occurrence as po
  inner join concept as cm
    on po.source_code = cm.concept_code
)

select
  p.person_id,
  p.visit_occurrence_id,
  p.procedure_date,
  p.procedure_datetime,
  pr1.provider_id as provider_id,
  p.procedure_source_value,
  p.procedure_type_concept_id,
  p.source_code,
  p.concept_id,
  last_edit_time,
  data_source
from procs as p
inner join vocab.CONCEPT as cn
  on p.concept_id = cn.concept_id
left join lth_bronze.vocab__provider as pr1
  on
    p.provider_id = case
      when isnumeric(p.provider_id) = 0
        then pr1.cons_org_code
      else pr1.provider_source_value
    end
where
  cn.domain_id = 'Procedure'
  and cn.invalid_reason is null;
