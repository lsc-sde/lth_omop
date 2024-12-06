{{
    config(
        materialized='view',
        tags = ['visit', 'bulk', 'visit_detail', 'staging', 'flex']
    )
}}

with ip_detail as (
  select
    patient_id,
    visit_id,
    visit_number,
    location_id,
    location_hx_time as checkin_datetime,
    coalesce(
      lead(location_hx_time) over (
        partition by visit_id order by location_hx_time
      ),
      discharge_date_time
    ) as checkout_datetime,
    last_edit_time,
    updated_at
  from lth_bronze.src_flex__visit_detail_ip 
),

ae_detail as (
  select
    patient_id,
    visit_id,
    visit_number,
    location_id,
    date_time_in as checkin_datetime,
    date_time_out as checkout_datetime,
    last_edit_time,
    updated_at
  from lth_bronze.src_flex__visit_detail_ae 
)

select
  a.patient_id,
  a.visit_number,
  visit_type,
  location_id,
  checkin_datetime,
  checkout_datetime,
  a.last_edit_time,
  a.updated_at,
  concat(
    coalesce(ft.first_visit_id, a.visit_id),
    row_number() over (
      partition by coalesce(ft.first_visit_id, a.visit_id)
      order by checkin_datetime
    )
  ) as visit_detail_id,
  coalesce(ft.first_visit_id, a.visit_id) as visit_id
from (
  select
    ae.*,
    case
      when ip.visit_id is not null then 'ERIP' else 'ER'
    end as visit_type
  from ae_detail as ae
  left join (select distinct visit_id from ip_detail) as ip
    on ip.visit_id = ae.visit_id
  union all
  select
    ip.*,
    case
      when ae.visit_id is not null then 'ERIP' else 'IP'
    end as visit_type
  from ip_detail as ip
  left join
    (select distinct visit_id from ae_detail)
      as ae
    on ip.visit_id = ae.visit_id
) as a
left join lth_bronze.stg_flex__facility_transfer as ft
  on a.visit_id = ft.visit_id
