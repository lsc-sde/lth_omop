
MODEL (
  name lth_bronze.visit_detail,
  kind FULL,
  cron '@daily',
);

select
  visit_detail_id::bigint as visit_detail_id,
  person_id::bigint as person_id,
  visit_detail_concept_id::bigint as visit_detail_concept_id,
  visit_detail_start_datetime::date as visit_detail_start_date,
  visit_detail_start_datetime::datetime as visit_detail_start_datetime,
  visit_detail_end_datetime::date as visit_detail_end_date,
  visit_detail_end_datetime::datetime as visit_detail_end_datetime,
  visit_detail_type_concept_id::bigint as visit_detail_type_concept_id,
  provider_id::bigint as provider_id,
  cs.care_site_id::bigint as care_site_id,
  visit_detail_source_value::varchar(50) as visit_detail_source_value,
  visit_detail_source_concept_id::bigint
    as visit_detail_source_concept_id,
  admitted_from_concept_id::bigint as admitted_from_concept_id,
  admitted_from_source_value::varchar(50) as admitted_from_source_value,
  discharged_to_source_value::varchar(50) as discharged_to_source_value,
  discharged_to_concept_id::bigint as discharged_to_concept_id,
  preceding_visit_detail_id::bigint as preceding_visit_detail_id,
  parent_visit_detail_id::bigint as parent_visit_detail_id,
  visit_occurrence_id::bigint as visit_occurrence_id,
  vd.source_system::varchar(20),
  vd.org_code::varchar(5)
from lth_bronze.vocab__visit_detail as vd
left join lth_bronze.care_site as cs
  on vd.location_id = cs.care_site_source_value