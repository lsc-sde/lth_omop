{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'metastases']
    )
}}

select *
from @catalaog_src.@schema_src.src_scr__metastases
