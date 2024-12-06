
MODEL (
  name lth_bronze.VISIT_DETAIL,
  kind FULL,
  cron '@daily',
);

select
  cast(visit_detail_id as bigint) as visit_detail_id,
  cast(person_id as bigint) as person_id,
  cast(visit_detail_concept_id as bigint) as visit_detail_concept_id,
  cast(visit_detail_start_datetime as date) as visit_detail_start_date,
  cast(visit_detail_start_datetime as datetime) as visit_detail_start_datetime,
  cast(visit_detail_end_datetime as date) as visit_detail_end_date,
  cast(visit_detail_end_datetime as datetime) as visit_detail_end_datetime,
  cast(visit_detail_type_concept_id as bigint) as visit_detail_type_concept_id,
  cast(provider_id as bigint) as provider_id,
  cast(cs.care_site_id as bigint) as care_site_id,
  cast(visit_detail_source_value as varchar(50)) as visit_detail_source_value,
  cast(visit_detail_source_concept_id as bigint)
    as visit_detail_source_concept_id,
  cast(admitted_from_concept_id as bigint) as admitted_from_concept_id,
  cast(admitted_from_source_value as varchar(50)) as admitted_from_source_value,
  cast(discharged_to_source_value as varchar(50)) as discharged_to_source_value,
  cast(discharged_to_concept_id as bigint) as discharged_to_concept_id,
  cast(preceding_visit_detail_id as bigint) as preceding_visit_detail_id,
  cast(parent_visit_detail_id as bigint) as parent_visit_detail_id,
  cast(visit_occurrence_id as bigint) as visit_occurrence_id
from lth_bronze.vocab__visit_detail as vd
left join lth_bronze.CARE_SITE as cs
  on vd.location_id = cs.care_site_source_value
