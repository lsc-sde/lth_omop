
MODEL (
  name lth_bronze.measurement,
  kind FULL,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

select
  vm.person_id as person_id,
  vm.target_concept_id::bigint as measurement_concept_id,
  vm.result_datetime as measurement_date,
  vm.result_datetime as measurement_datetime,
  vm.result_datetime as measurement_time,
  vm.type_concept_id::bigint as measurement_type_concept_id,
  vm.operator_concept_id::bigint as operator_concept_id,
  vm.value_as_number as value_as_number,
  vm.value_as_concept_id::bigint as value_as_concept_id,
  vm.unit_concept_id::bigint as unit_concept_id,
  vm.range_low as range_low,
  vm.range_high as range_high,
  pr.provider_id::bigint as provider_id,
  vm.visit_occurrence_id::bigint as visit_occurrence_id,
  vm.visit_detail_id::bigint as visit_detail_id,
  vm.source_value as measurement_source_value,
  vm.measurement_source_concept_id::bigint as measurement_source_concept_id,
  vm.unit_source_value as unit_source_value,
  vm.unit_source_concept_id::bigint as unit_source_concept_id,
  vm.value_source_value as value_source_value,
  vm.meas_event_field_concept_id::bigint as meas_event_field_concept_id,
  vm.measurement_event_id::varchar(80),
  @generate_surrogate_key(vm.source_system,vm.person_id,vm.measurement_event_id,vm.target_concept_id,source_value,value_source_value) as unique_key,
  vm.org_code,
  vm.source_system,
  vm.updated_at as last_edit_time,
  getdate() as insert_date_time
from lth_bronze.vocab__measurement as vm
left join lth_bronze.PROVIDER as pr
  on vm.provider_id = pr.provider_source_value
