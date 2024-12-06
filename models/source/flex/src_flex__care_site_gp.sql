/*
OMOP CARE SITE QUERY
CONTEXT:
- Extraction of care sites and locations from Flex Warehouse.
RESULT EXPECTATION:
- List of Flex locations (wards, areas and OP clinics).
ASSUMPTION:
- All physical locations are held in ud_master.location.
- Outpatient clinic are all listed in ud_master.resource_entity
*/

{{
  config(
    materialized = 'view',
    tags = ['lookup', 'dimension', 'care_site', 'bulk', 'source', 'flex']
    )
}}

select
  care_site_id,
  care_site_name,
  care_site_source_value,
  care_site_location
from {{ source('omop_source', 'src_flex__care_site_gp') }}
