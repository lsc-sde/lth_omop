
MODEL (
  name lth_bronze.fr__specimen_to_measurement,
  kind FULL,
  cron '@daily',
);

select
  s.domain_concept_id_1,
  s.fact_id_1,
  s.domain_concept_id_2,
  s.fact_id_2,
  s.relationship_concept_id,
  s.specimen_event_id,
  s.measurement_key,
  s.unique_key,
  s.last_edit_time
from lth_bronze.cdc_fr__specimen_to_measurement as s
