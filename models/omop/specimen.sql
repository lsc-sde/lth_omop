
MODEL (
  name lth_bronze.specimen,
  kind FULL,
  cron '@daily'
);

select
  row_number() over (order by NEWID())::bigint as specimen_id,
  person_id::bigint as person_id,
  specimen_concept_id::bigint as specimen_concept_id,
  specimen_type_concept_id::bigint as specimen_type_concept_id,
  specimen_date::date as specimen_date,
  specimen_date::datetime as specimen_datetime,
  NULL::float as quantity,
  NULL::bigint as unit_concept_id,
  anatomic_site_concept_id::bigint as anatomic_site_concept_id,
  NULL::bigint as disease_status_concept_id,
  specimen_source_id::varchar(50) as specimen_source_id,
  specimen_source_value::varchar(50) as specimen_source_value,
  NULL::varchar(50) as unit_source_value,
  anatomic_site_source_value::varchar(50) as anatomic_site_source_value,
  NULL::varchar(50) as disease_status_source_value,
  measurement_event_id as specimen_event_id,
  updated_at::datetime as updated_at
from lth_bronze.vocab__specimen