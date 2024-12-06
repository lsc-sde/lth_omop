{{
  config(
    materialized = "table",
    tags = ['omop', 'drugs']
    )
}}

select
  cast(row_number() over (order by newid()) as bigint) as drug_exposure_id,
  cast(de.person_id as bigint) as person_id,
  cast(de.target_concept_id as bigint) as drug_concept_id,
  cast(drug_exposure_start_datetime as date) as drug_exposure_start_date,
  cast(drug_exposure_start_datetime as datetime)
    as drug_exposure_start_datetime,
  cast(drug_exposure_start_datetime as date) as drug_exposure_end_date,
  cast(drug_exposure_start_datetime as datetime) as drug_exposure_end_datetime,
  cast(null as date) as verbatim_end_date,
  cast(drug_type_concept_id as bigint) as drug_type_concept_id,
  cast(null as varchar(20)) as stop_reason,
  cast(null as int) as refills,
  cast(null as float) as quantity,
  cast(null as int) as days_supply,
  cast(null as varchar(8000)) as sig,
  cast(route_concept_id as bigint) as route_concept_id,
  cast(null as varchar(50)) as lot_number,
  cast(pr.provider_id as bigint) as provider_id,
  cast(visit_occurrence_id as bigint) as visit_occurrence_id,
  cast(null as bigint) as visit_detail_id,
  cast(drug_source_value as varchar(50)) as drug_source_value,
  cast(null as bigint) as drug_source_concept_id,
  cast(adm_route as varchar(50)) as route_source_value,
  cast(null as varchar(50)) as dose_unit_source_value
from {{ ref('vocab__drug_exposure') }} as de
left join {{ ref('PROVIDER') }} as pr
  on de.provider_id = pr.provider_source_value
