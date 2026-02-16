MODEL (
  name vcb.vocab__death,
  kind FULL,
  cron '@daily'
);

SELECT
  person_id,
  death_datetime,
  32817 AS death_type_concept_id,
  death_datetime::DATE AS death_date,
  source_system,
  org_code
FROM stg.stg__person
WHERE
  NOT death_datetime IS NULL
