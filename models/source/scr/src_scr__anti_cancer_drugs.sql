{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'drugs']
    )
}}

select *
from {{ source('omop_source', 'src_scr__anti_cancer_drugs') }}
