
MODEL (
  name lth_bronze.stg__provider,
  kind FULL,
  cron '@daily',
);

select
  translate(provider_source_value, '0123456789', '0239687154') as provider_id,
  care_site_id,
  provider_name,
  provider_source_value,
  specialty_source_value,
  cons_org_code,
  source_system,
  org_code
from lth_bronze.stg_flex__provider 
