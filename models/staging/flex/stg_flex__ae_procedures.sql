
MODEL (
  name lth_bronze.stg_flex__ae_procedures,
  kind FULL,
  cron '@daily',
);

select
  visit_id,
  patient_id,
  126 as provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  source_system::varchar(20),
  org_code::varchar(5),
  last_edit_time,
  updated_at,
  isnull(
    case
      when value like '%||%' then null
      else
        convert(
          datetime,
          left(
            replace(substring(value, charindex('|', value) + 1, 1000), '|', ' '), --noqa
            11
          )
          + ':'
          + replace(substring(value, charindex('|', value) + 12, 2), '|', ' ')
        )
    end,
    admission_date_time
  ) as procedure_datetime,
  left(value, charindex('|', value) - 1) as list
from lth_bronze.src_flex__ae_procedures 
cross apply string_split(list, '~')
