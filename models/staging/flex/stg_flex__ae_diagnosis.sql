
MODEL (
  name lth_bronze.stg_flex__ae_diagnosis,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (last_edit_time, '%Y-%m-%d %H:%M:%S.%f'),
    batch_size 30
  ),
  cron '@daily',
  audits (
    not_null(columns := (visit_id, patient_id, last_edit_time, updated_at, diagnosis_list))
  ),
  grain (patient_id, visit_id, diagnosis_list)
);

select
  visit_id,
  patient_id,
  126 as provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  last_edit_time,
  updated_at,
  s2.value as diagnosis_list
from (
  select
    visit_id,
    patient_id,
    activation_time,
    admission_date_time,
    discharge_date_time,
    last_edit_time,
    updated_at,
    s1.value as diag_list
  from lth_bronze.src_flex__ae_diagnosis
  where last_edit_time between @start_ds and @end_ds
  cross apply string_split(diag_list, '~') as s1
) as t
cross apply string_split(diag_list, '|') as s2
where
  len(value) > 1
  and value not in ('410605003')
  and last_edit_time between @start_ds and @end_ds
