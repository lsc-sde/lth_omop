{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'assessments']
    )
}}

select *
from @catalaog_src.@schema_src.src_scr__initial_assessments
