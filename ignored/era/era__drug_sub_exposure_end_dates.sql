
MODEL (
  name lth_bronze.era__drug_sub_exposure_end_dates,
  kind FULL,
  cron '@daily',
);

select
  person_id,
  ingredient_concept_id,
  event_date as end_date
from
  (
    select
      person_id,
      ingredient_concept_id,
      event_date,
      event_type,
      max(start_ordinal) over (
        partition by person_id, ingredient_concept_id
        order by event_date, event_type rows unbounded preceding
      ) as start_ordinal,
      row_number() over (
        partition by person_id, ingredient_concept_id
        order by event_date, event_type
      ) as overall_ord
    from (
      select
        person_id,
        ingredient_concept_id,
        drug_exposure_start_date as event_date,
        -1 as event_type,
        row_number() over (
          partition by person_id, ingredient_concept_id
          order by drug_exposure_start_date
        ) as start_ordinal
      from lth_bronze.era__drug_pre_target 

      union all

      select
        person_id,
        ingredient_concept_id,
        drug_exposure_end_date,
        1 as event_type,
        null
      from lth_bronze.era__drug_pre_target 
    ) as RAWDATA
  ) as e
where (2 * e.start_ordinal) - e.overall_ord = 0
