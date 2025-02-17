
MODEL (
  name lth_bronze.vocab__specimen,
  kind FULL,
  cron '@daily',
);

select
  person_id,
  vc.target_concept_id as specimen_concept_id,
  32817 as specimen_type_concept_id,
  order_date as specimen_date,
  vc.target_concept_name,
  vc_s.target_concept_id as anatomic_site_concept_id,
  order_number as specimen_source_id,
  site as specimen_source_value,
  qualifier as anatomic_site_source_value,
  measurement_event_id,
  updated_at
from lth_bronze.stg__specimen as sp
inner join
  (
    select
      target_concept_id,
      target_concept_name,
      source_code
    from lth_bronze.vocab__source_to_concept_map
    where
      source_system = 'swisslab'
      and concept_group = 'specimen_type'
  ) as vc
  on sp.site = vc.source_code
left join
  (
    select
      target_concept_id,
      source_code
    from lth_bronze.vocab__source_to_concept_map
    where
      source_system = 'swisslab'
      and concept_group = 'anatomical_site'
  ) as vc_s
  on sp.qualifier = vc_s.source_code