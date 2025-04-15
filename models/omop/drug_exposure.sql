
MODEL (
  name lth_bronze.drug_exposure,
  kind FULL,
  cron '@daily',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key unique_key
  )
);

select
  abs(cast(cast(
    @generate_surrogate_key(
      person_id,visit_occurrence_id,de.target_concept_id,dosage,drug_exposure_start_datetime,adm_route,last_edit_time
      )
   as varbinary(8)) as bigint)) as drug_exposure_id,
  de.person_id::bigint as person_id,
  de.target_concept_id::bigint as drug_concept_id,
  drug_exposure_start_datetime::date as drug_exposure_start_date,
  drug_exposure_start_datetime::datetime as drug_exposure_start_datetime,
  drug_exposure_start_datetime::date as drug_exposure_end_date,
  drug_exposure_start_datetime::datetime as drug_exposure_end_datetime,
  null::date as verbatim_end_date,
  drug_type_concept_id::bigint as drug_type_concept_id,
  null::varchar(20) as stop_reason,
  null::int as refills,
  null::float as quantity,
  null::int as days_supply,
  null::varchar(8000) as sig,
  route_concept_id::bigint as route_concept_id,
  null::varchar(50) as lot_number,
  pr.provider_id::bigint as provider_id,
  visit_occurrence_id::bigint as visit_occurrence_id,
  null::bigint as visit_detail_id,
  drug_source_value::varchar(50) as drug_source_value,
  null::bigint as drug_source_concept_id,
  adm_route::varchar(50) as route_source_value,
  null::varchar(50) as dose_unit_source_value,
  @generate_surrogate_key(
    person_id,visit_occurrence_id,de.target_concept_id,dosage,drug_exposure_start_datetime,adm_route,last_edit_time
    ) as unique_key,
  de.org_code::varchar(5),
  de.source_system::varchar(20),
  de.last_edit_time::datetime,
  getdate()::datetime as insert_date_time
from lth_bronze.vocab__drug_exposure as de
left join lth_bronze.PROVIDER as pr
  on de.provider_id = pr.provider_source_value