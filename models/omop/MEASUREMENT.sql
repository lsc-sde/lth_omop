{{
  config(
    materialized = "incremental",
    tags = ['omop', 'measurement'],
    unique_key= ['unique_key']
    )
}}

{%- if is_incremental() %}
{%- set max_id = max_existing('measurement_id') %}
{%- else %}
   {%- set max_id = var('start_id') %}
 {%- endif %}

select
  {{ max_id }} + row_number() over (order by NewID()) as measurement_id,
  vm.person_id as person_id,
  cast(vm.target_concept_id as bigint) as measurement_concept_id,
  vm.result_datetime as measurement_date,
  vm.result_datetime as measurement_datetime,
  vm.result_datetime as measurement_time,
  cast(vm.type_concept_id as bigint) as measurement_type_concept_id,
  cast(vm.operator_concept_id as bigint) as operator_concept_id,
  vm.value_as_number as value_as_number,
  cast(vm.value_as_concept_id as bigint) as value_as_concept_id,
  cast(vm.unit_concept_id as bigint) as unit_concept_id,
  vm.range_low as range_low,
  vm.range_high as range_high,
  cast(pr.provider_id as bigint) as provider_id,
  cast(vm.visit_occurrence_id as bigint) as visit_occurrence_id,
  cast(vm.visit_detail_id as bigint) as visit_detail_id,
  vm.source_value as measurement_source_value,
  cast(vm.measurement_source_concept_id as bigint) as measurement_source_concept_id,
  vm.unit_source_value as unit_source_value,
  cast(vm.unit_source_concept_id as bigint) as unit_source_concept_id,
  vm.value_source_value as value_source_value,
  cast(vm.meas_event_field_concept_id as bigint) as meas_event_field_concept_id,
  vm.measurement_event_id,
  {{ dbt_utils.generate_surrogate_key([
        'vm.datasource',
        'vm.person_id',
        'vm.measurement_event_id',
        'vm.target_concept_id',
        'source_value',
        'value_source_value'
        ])
    }} as unique_key,
  vm.datasource,
  vm.updated_at
from {{ ref('vocab__measurement') }} as vm
left join {{ ref('PROVIDER') }} as pr
  on vm.provider_id = pr.provider_source_value
