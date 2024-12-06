{{
  config(
    materialized = "table",
    tags = ['provider', 'staging'],
    docs = {
        'name': 'stg__provider',
        'description': 'Provider view for all sources'
    }
    )
}}

select
  translate(provider_source_value, '0123456789', '0239687154') as provider_id,
  care_site_id,
  provider_name,
  provider_source_value,
  specialty_source_value,
  cons_org_code
from {{ ref('stg_flex__provider') }}
