
MODEL (
  name stg.stg__specimen,
  kind FULL,
  cron '@daily',
);

with person as (
	select
		mpi.nhs_number,
		mpi.person_id,
		mpi.person_source_value
	from stg.stg__master_patient_index mpi
	where mpi.current_record = 1
	and exists (
		select 1 from stg.stg_sl__bacteriology sl where sl.nhs_number = mpi.nhs_number
  )
)

select distinct
  mpi.person_id,
  mpi.person_source_value,
  sl_ba.nhs_number::varchar(20),
  sl_ba.order_date,
  sl_ba.site::varchar(100),
  qualifier::varchar(20),
  order_number::varchar(20),
  measurement_event_id,
  org_code::varchar(5),
  source_system::varchar(20),
  updated_at
from stg.stg_sl__bacteriology as sl_ba
inner join person as mpi
  on sl_ba.nhs_number = mpi.nhs_number
where
  sl_ba.site is not null
  and sl_ba.order_date is not null
