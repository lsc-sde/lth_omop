{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'assessments']
    )
}}

select *
from {{ source('omop_source', 'src_scr__initial_assessments') }}
