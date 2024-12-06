{{
    config(
        tags = ['vocab', 'death', 'person']
    )
}}

select
  person_id,
  death_datetime,
  32817 as death_type_concept_id,
  convert(date, death_datetime) as death_date
from lth_bronze.stg__death 
where death_datetime is not null
