{{
  config(
    materialized = "view"
    )
}}

with cdc as (
  select min(updated_at) as updated_at
  from lth_bronze.cdc__updated_at 
  where
    model in ('fr__specimen_to_measurement')
),

specimen as (
select * from lth_bronze.SPECIMEN s 
where s.updated_at > (
    select dateadd(d, -5, updated_at) from cdc
  )
),

measurement as (
select * from lth_bronze.MEASUREMENT s 
where measurement_event_id in (select specimen_event_id from specimen)
)

select distinct
  1147306 as domain_concept_id_1,
  specimen_id as fact_id_1,
  1147330 as domain_concept_id_2,
  ms.measurement_id as fact_id_2,
  32669 as relationship_concept_id,
  specimen_event_id,
  ms.unique_key as measurement_key,
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(max), concat(coalesce(cast(specimen_id as VARCHAR(MAX)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(ms.measurement_id as VARCHAR(MAX)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(s.updated_at as VARCHAR(MAX)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
  as unique_key,
  s.updated_at as last_edit_time
from specimen as s
inner join measurement as ms
  on s.specimen_event_id = ms.measurement_event_id
where
  s.updated_at > (
    select dateadd(d, -5, updated_at) from cdc
  )
  and s.updated_at < (
    select dateadd(day, 90, updated_at) from cdc
  )
  and s.updated_at <= getdate()
