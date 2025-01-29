
MODEL (
  name lth_bronze.stg_flex__visit_detail,
  kind FULL,
  cron '@daily',
);

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
    source_system,
    org_code,
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
    source_system,
    org_code,
    last_edit_time,
    updated_at
  from lth_bronze.src_flex__visit_detail_ae 
)

select
  a.patient_id::BIGINT,
  a.visit_number::VARCHAR(50),
  visit_type::VARCHAR(10),
  location_id::VARCHAR(50),
  checkin_datetime::DATETIME,
  checkout_datetime::DATETIME,  
  a.source_system::varchar(20),
  a.org_code::varchar(5),
  a.last_edit_time::DATETIME,
  a.updated_at::DATETIME,
  concat(
    coalesce(ft.first_visit_id, a.visit_id),
    row_number() over (
      partition by coalesce(ft.first_visit_id, a.visit_id)
      order by checkin_datetime
    )
  )::BIGINT as visit_detail_id,
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
