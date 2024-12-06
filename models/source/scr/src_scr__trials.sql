{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'trials']
    )
}}

select *
from {{ source('omop_source', 'src_scr__trials') }}
