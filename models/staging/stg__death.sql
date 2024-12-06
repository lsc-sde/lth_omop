{{
  config(
    materialized = "view",
    tags = ['person', 'staging', 'death', 'person'],
    docs = {
        'name': 'stg__death',
        'description': 'Death view for all sources'
    }
    )
}}

select
  person_id,
  person_source_value,
  death_datetime
from {{ ref('stg__person') }}
where death_datetime is not null
