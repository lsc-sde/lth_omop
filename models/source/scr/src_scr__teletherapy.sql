{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'referrals']
    )
}}

select *
from {{ source('omop_source', 'src_scr__teletherapy') }}
