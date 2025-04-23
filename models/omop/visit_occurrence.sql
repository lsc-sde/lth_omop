
MODEL (
  name lth_bronze.visit_occurrence,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key visit_occurrence_id
  )
);

select
  visit_occurrence_id::bigint as visit_occurrence_id,
  person_id::bigint as person_id,
  visit_concept_id::bigint as visit_concept_id,
  visit_start_datetime::date as visit_start_date,
  visit_start_datetime::datetime as visit_start_datetime,
  visit_end_datetime::date as visit_end_date,
  visit_end_datetime::datetime as visit_end_datetime,
  visit_type_concept_id::bigint as visit_type_concept_id,
  pr.provider_id::bigint as provider_id,
  cs.care_site_id::bigint as care_site_id,
  visit_source_value::varchar(50) as visit_source_value,
  visit_source_concept_id::bigint as visit_source_concept_id,
  admitted_from_concept_id::bigint as admitted_from_concept_id,
  admitted_from_source_value::varchar(50) as admitted_from_source_value,
  discharged_to_concept_id::bigint as discharged_to_concept_id,
  discharged_to_source_value::varchar(50) as discharged_to_source_value,
  lag(visit_occurrence_id)
      over (partition by person_id order by visit_start_datetime) as preceding_visit_occurrence_id,
  vo.visit_type_id,
  vo.visit_status_id,  
  vo.source_system::varchar(20),
  vo.org_code::varchar(5),
  vo.last_edit_time,
  vo.updated_at
from lth_bronze.vocab__visit_occurrence as vo
left join lth_bronze.provider as pr
  on vo.provider_id::varchar = pr.provider_source_value
left join
  (
    select *
    from lth_bronze.care_site
    where place_of_service_source_value = 'NHS Trust'
  ) as cs
  on vo.care_site_id::varchar = cs.care_site_source_value