{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'breast_markers']
    )
}}

select *
from @catalaog_src.@schema_src.src_scr__breast_markers
