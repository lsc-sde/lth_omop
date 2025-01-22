
MODEL (
  name lth_bronze.cdc__updated_at_default,
  kind SEED (
    path '$root/seeds/cdc__updated_at_default.csv'
  ),
  description 'Initial updated_at timestamps for incremental updates. Unlikely to be needed in SQLMesh.'
   columns (
    model VARCHAR(50),
    datasource VARCHAR(50),
    updated_at DATETIME
  )
);
