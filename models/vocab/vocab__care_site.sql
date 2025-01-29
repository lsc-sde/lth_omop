
MODEL (
  name lth_bronze.vocab__care_site,
  kind FULL,
  cron '@daily',
);

select
  care_site_id,
  care_site_name,
  care_site_source_value,
  postcode,
  place_of_service_source_value,
  location_source_value,
  case
    when (
      care_site_source_value like '232~%'
      or care_site_source_value like '567~%'
      or care_site_source_value like '28~%'
      or care_site_source_value in ('28', '232', '567')
    )
    and care_site_name like '%GTD%' then '8782'
    when (
      care_site_source_value like '232~%'
      or care_site_source_value like '567~%'
      or care_site_source_value like '28~%'
      or care_site_source_value in ('28', '232', '567')
    )
    and care_site_name not like '%GTD%' then '8870'
    when place_of_service_source_value = 'Quadramed Location' then '8717'
    when place_of_service_source_value = 'Outpatient Clinic' then '8756'
    when place_of_service_source_value = 'GP Practice' then null
    when place_of_service_source_value = 'NHS Trust' then '8717'
  end::bigint as place_of_service_concept_id,
  source_system,
  org_code
from lth_bronze.stg__care_site