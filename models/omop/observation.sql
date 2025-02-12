
MODEL (
  name lth_bronze.observation,
  kind FULL,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

select
  --{{ max_id }} + row_number() over (order by NewID()) as observation_id,
  person_id,
  target_concept_id::bigint as observation_concept_id,
  result_datetime::DATE as observation_date,
  result_datetime::DATETIME as observation_datetime,
  type_concept_id::bigint as observation_type_concept_id,
  try_cast(value_as_number as float) as value_as_number,
  value_as_string::VARCHAR(60) as value_as_string,
  value_as_concept_id::bigint as value_as_concept_id,
  qualifier_concept_id::bigint as qualifier_concept_id,
  unit_concept_id::bigint as unit_concept_id,
  pr.provider_id::bigint as provider_id,
  visit_occurrence_id::bigint as visit_occurrence_id,
  visit_detail_id::bigint as visit_detail_id,
  source_value::VARCHAR(50) as observation_source_value,
  observation_source_concept_id::bigint as observation_source_concept_id,
  unit_source_value::VARCHAR(50) as unit_source_value,
  qualifier_source_value::VARCHAR(50) as qualifier_source_value,
  value_source_value::VARCHAR(50) as value_source_value,
  observation_event_id::VARCHAR(50) as observation_event_id,
  obs_event_field_concept_id::bigint as obs_event_field_concept_id,
  @generate_surrogate_key(vo.source_system,person_id,observation_event_id,visit_occurrence_id,source_value,value_source_value,result_datetime,target_concept_id,qualifier_concept_id) as unique_key,
  vo.org_code,
  vo.source_system,
  last_edit_time,
  getdate() as insert_date_time
from lth_bronze.vocab__observation as vo
left join lth_bronze.PROVIDER as pr
  on vo.provider_id = pr.provider_source_value