MODEL (
  name lth_bronze.stg_flex__ae_procedures,
  kind FULL,
  cron '@daily'
);

SELECT
  visit_id,
  patient_id,
  126 AS provider_id,
  activation_time,
  admission_date_time,
  discharge_date_time,
  org_code::VARCHAR(5),
  last_edit_time,
  updated_at,
  isnull(
    CASE
      WHEN value LIKE '%||%'
      THEN NULL
      ELSE convert(
        DATETIME,
        left(
          replace(substring(value, charindex('|', value) + 1, 1000), '|', ' ') /* noqa */,
          11
        ) + ':' + replace(substring(value, charindex('|', value) + 12, 2), '|', ' ')
      )
    END,
    admission_date_time
  ) AS procedure_datetime,
  left(value, charindex('|', value) - 1) AS list,
  'flex_ae' AS source_system
FROM lth_bronze.src_flex__ae_procedures CROSS APPLY string_split(list, '~')
