
MODEL (
  name lth_bronze.cdc__updated_at_default,
  kind SEED (
    path '$root/seeds/cdc__updated_at_default.csv'
  ),
   columns (
    model VARCHAR(50),
    datasource VARCHAR(50),
    updated_at DATETIME
  )
);
