{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'metastases']
    )
}}

select *
from {{ source('omop_source', 'src_scr__metastases') }}
