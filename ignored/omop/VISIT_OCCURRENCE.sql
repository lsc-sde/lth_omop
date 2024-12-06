
MODEL (
  name lth_bronze.VISIT_OCCURRENCE,
  kind FULL,
  cron '@daily',
);

select
  cast(visit_occurrence_id as bigint) as visit_occurrence_id,
  cast(person_id as bigint) as person_id,
  cast(visit_concept_id as bigint) as visit_concept_id,
  cast(visit_start_datetime as date) as visit_start_date,
  cast(visit_start_datetime as datetime) as visit_start_datetime,
  cast(visit_end_datetime as date) as visit_end_date,
  cast(visit_end_datetime as datetime) as visit_end_datetime,
  cast(visit_type_concept_id as bigint) as visit_type_concept_id,
  cast(pr.provider_id as bigint) as provider_id,
  cast(cs.care_site_id as bigint) as care_site_id,
  cast(visit_source_value as varchar(50)) as visit_source_value,
  cast(visit_source_concept_id as bigint) as visit_source_concept_id,
  cast(admitted_from_concept_id as bigint) as admitted_from_concept_id,
  cast(admitted_from_source_value as varchar(50)) as admitted_from_source_value,
  cast(discharged_to_concept_id as bigint) as discharged_to_concept_id,
  cast(discharged_to_source_value as varchar(50)) as discharged_to_source_value,
  cast(
    lag(visit_occurrence_id)
      over (partition by person_id order by visit_start_datetime)
    as bigint
  ) as preceding_visit_occurrence_id,
  vo.visit_type_id,
  vo.visit_status_id,
  vo.last_edit_time,
  vo.updated_at
from lth_bronze.vocab__visit_occurrence as vo
left join lth_bronze.PROVIDER as pr
  on cast(vo.provider_id as varchar) = pr.provider_source_value
left join
  (
    select *
    from lth_bronze.CARE_SITE 
    where place_of_service_source_value = 'NHS Trust'
  ) as cs
  on cast(vo.care_site_id as varchar) = cs.care_site_source_value
