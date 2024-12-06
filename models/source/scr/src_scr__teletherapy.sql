{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'referrals']
    )
}}

select *
from @catalaog_src.@schema_src.src_scr__teletherapy
