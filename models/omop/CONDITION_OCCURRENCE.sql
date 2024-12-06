{{
  config(
    materialized = "table",
    tags = ['omop', 'condition']
    )
}}

select
  cast(row_number() over (order by NewID()) as bigint)
    as condition_occurrence_id,
  cast(person_id as bigint) as person_id,
  cast(condition_concept_id as bigint) as condition_concept_id,
  cast(condition_start_date as date) as condition_start_date,
  cast(condition_start_date as datetime) as condition_start_datetime,
  cast(condition_end_date as date) as condition_end_date,
  cast(condition_end_date as datetime) as condition_end_datetime,
  cast(condition_type_concept_id as bigint) as condition_type_concept_id,
  cast(condition_status_concept_id as bigint) as condition_status_concept_id,
  cast(null as varchar(20)) as stop_reason,
  cast(Isnull(pr1.provider_id, pr2.provider_id) as bigint) as provider_id,
  cast(visit_occurrence_id as bigint) as visit_occurrence_id,
  cast(null as bigint) as visit_detail_id,
  cast(condition_source_value as varchar(50)) as condition_source_value,
  cast(condition_source_concept_id as bigint) as condition_source_concept_id,
  cast(null as varchar(50)) as condition_status_source_value,
  datasource
from {{ ref('vocab__condition_occurrence') }} as c
left join {{ ref('vocab__provider') }} as pr1
  on
    c.provider_id = pr1.cons_org_code
    and Isnumeric(c.provider_id) = 0
left join {{ ref('vocab__provider') }} as pr2
  on
    c.provider_id = pr2.provider_source_value
    and Isnumeric(c.provider_id) = 1
