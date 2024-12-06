{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'drugs']
    )
}}

select *
from @catalaog_src.@schema_src.src_scr__anti_cancer_drugs
