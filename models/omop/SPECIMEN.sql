{{
  config(
    materialized = "table",
    tags = ['omop', 'specimen']
    )
}}

select
  cast(row_number() over (order by NEWID()) as bigint) as specimen_id,
  cast(person_id as bigint) as person_id,
  cast(specimen_concept_id as bigint) as specimen_concept_id,
  cast(specimen_type_concept_id as bigint) as specimen_type_concept_id,
  cast(specimen_date as date) as specimen_date,
  cast(specimen_date as datetime) as specimen_datetime,
  cast(NULL as float) as quantity,
  cast(NULL as bigint) as unit_concept_id,
  cast(anatomic_site_concept_id as bigint) as anatomic_site_concept_id,
  cast(NULL as bigint) as disease_status_concept_id,
  cast(specimen_source_id as varchar(50)) as specimen_source_id,
  cast(specimen_source_value as varchar(50)) as specimen_source_value,
  cast(NULL as varchar(50)) as unit_source_value,
  cast(anatomic_site_source_value as varchar(50)) as anatomic_site_source_value,
  cast(NULL as varchar(50)) as disease_status_source_value,
  cast(measurement_event_id as varchar(50)) as specimen_event_id,
  cast(updated_at as datetime) as updated_at
from lth_bronze.vocab__specimen 
