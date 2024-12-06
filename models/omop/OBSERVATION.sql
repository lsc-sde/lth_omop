{{
  config(
    materialized = "incremental",
    tags = ['omop', 'observation'],
    unique_key = ['unique_key']

    )
}}

{%- if is_incremental() %}
{%- set max_id = max_existing('observation_id') %}
{%- else %}
   {%- set max_id = var('start_id') %}
 {%- endif %}

select
  {{ max_id }} + row_number() over (order by NewID()) as observation_id,
  person_id,
  cast(target_concept_id as bigint) as observation_concept_id,
  cast(result_datetime as DATE) as observation_date,
  cast(result_datetime as DATETIME) as observation_datetime,
  cast(type_concept_id as bigint) as observation_type_concept_id,
  TRY_CAST(value_as_number as FLOAT) as value_as_number,
  cast(value_as_string as VARCHAR(60)) as value_as_string,
  cast(value_as_concept_id as bigint) as value_as_concept_id,
  cast(qualifier_concept_id as bigint) as qualifier_concept_id,
  cast(unit_concept_id as bigint) as unit_concept_id,
  cast(pr.provider_id as bigint) as provider_id,
  cast(visit_occurrence_id as bigint) as visit_occurrence_id,
  cast(visit_detail_id as bigint) as visit_detail_id,
  cast(source_value as VARCHAR(50)) as observation_source_value,
  cast(observation_source_concept_id as bigint) as observation_source_concept_id,
  cast(unit_source_value as VARCHAR(50)) as unit_source_value,
  cast(qualifier_source_value as VARCHAR(50)) as qualifier_source_value,
  cast(value_source_value as VARCHAR(50)) as value_source_value,
  cast(observation_event_id as VARCHAR(50)) as observation_event_id,
  cast(obs_event_field_concept_id as bigint) as obs_event_field_concept_id,
  {{ dbt_utils.generate_surrogate_key([
        'datasource',
        'person_id',
        'observation_event_id',
        'visit_occurrence_id',
        'source_value',
        'value_source_value',
        'result_datetime',
        'target_concept_id',
        'qualifier_concept_id'
        ])
    }} as unique_key,
  datasource,
  updated_at
from {{ ref('vocab__observation') }} as vo
left join {{ ref('PROVIDER') }} as pr
  on vo.provider_id = pr.provider_source_value
