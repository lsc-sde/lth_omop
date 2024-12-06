{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'trials']
    )
}}

select *
from @catalaog_src.@schema_src.src_scr__trials
