{{
    config(
        materialized='table',
        tags = ['bulk', 'source', 'scr', 'breast_markers']
    )
}}

select *
from {{ source('omop_source', 'src_scr__breast_markers') }}
