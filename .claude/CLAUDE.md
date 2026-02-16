# CLAUDE.md - LTH OMOP ETL Project Instructions

This file provides context and guidelines for Claude when working on the LTH OMOP ETL project.

## Project Summary

This project transforms healthcare data from multiple EHR source systems (Flex, SwissLab, BI Referrals, Somerset Cancer Registry) into the **OMOP Common Data Model** bronze layer using **SQLMesh**.

### Tech Stack
- **SQLMesh** v0.230+ - Data transformation framework
- **SQL Server (MSSQL)** - Target database
- **SQLGlot** - SQL transpilation (use `::` casting syntax)
- **Python 3.12+** - Python models and macros
- **uv** - Package management

### Key Conventions
- Schema prefix: `lth_bronze`
- Default model kind: `VIEW`
- SQL dialect: `tsql` with case-sensitive normalization
- Use `::` for datatype casting (e.g., `column::BIGINT`)

---

## Project Structure

```
lth_omop/
├── config.py                    # SQLMesh configuration
├── external_models.yaml         # External source table definitions
├── macros/                      # Reusable SQL macros
│   ├── before_after.py          # Pre/post execution hooks
│   ├── source_to_concept_map.py # Vocabulary mapping generator
│   └── utils.py                 # Utility macros
├── models/
│   ├── source/                  # Source views (src_*)
│   │   ├── flex/                # Flex EHR sources
│   │   ├── swisslab/            # SwissLab sources
│   │   └── bi/                  # BI system sources
│   ├── staging/                 # Staging transformations (stg_*)
│   ├── vocab/                   # Vocabulary mappings (vocab_*)
│   ├── cdc/                     # Change data capture models
│   ├── external/                # External table wrappers (ext_*)
│   ├── seeds/                   # Seed model definitions
│   └── omop/                    # Final OMOP CDM tables
├── seeds/                       # CSV files for seed data
├── audits/                      # Custom data audits
└── tests/                       # Unit tests
```

---

## Data Flow Pipeline

```
external_models.yaml → source/ (src_*) → staging/ (stg_*) → vocab/ (vocab_*) → omop/
                           ↑
                       seeds/ (mappings)
```

| Layer    | Prefix   | Kind | Purpose                         |
| -------- | -------- | ---- | ------------------------------- |
| External | `ext__`  | FULL | Wrap external tables            |
| Source   | `src_`   | VIEW | Direct views on source data     |
| Staging  | `stg_`   | FULL | Clean, deduplicate, standardize |
| Vocab    | `vocab_` | VIEW | Apply vocabulary mappings       |
| OMOP     | (none)   | FULL | Final OMOP CDM tables           |
| Seeds    | Various  | SEED | Static mapping/reference data   |

---

## SQLMesh Model Kinds

### FULL
Complete table rebuild each run. Use for staging and OMOP tables.

```sql
MODEL (
  name lth_bronze.person,
  kind FULL,
  cron '@daily'
);
```

### VIEW
Creates a database view. Default for source and vocab layers.

```sql
MODEL (
  name src.src_flex__person,
  kind VIEW,
  cron '@daily'
);
```

### INCREMENTAL_BY_TIME_RANGE
Appends data based on time column. Use for large fact tables.

```sql
MODEL (
  name lth_bronze.events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date,
    batch_size 30,
    lookback 7
  ),
  cron '@daily'
);

SELECT * FROM source WHERE event_date BETWEEN @start_date AND @end_date
```

### INCREMENTAL_BY_UNIQUE_KEY
Upserts based on unique key. Use for CDC patterns.

```sql
MODEL (
  name stg.cdc_dimension,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [id],
    when_matched WHEN MATCHED THEN UPDATE SET target.col = source.col
  ),
  cron '@daily'
);
```

### SEED
Loads from CSV files for static mappings.

```sql
MODEL (
  name lth_bronze.flex__patient_demographics,
  kind SEED (
    path '$root/seeds/flex__patient_demographics.csv'
  ),
  description 'Vocabulary mappings for gender and ethnicity'
);
```

---

## SQL Coding Standards

### Datatype Casting
Use `::` syntax - SQLGlot transpiles to MSSQL:

```sql
SELECT
  id::BIGINT AS id,
  name::VARCHAR(100) AS name,
  amount::DECIMAL(18, 2) AS amount,
  created_at::DATETIME AS created_at,
  NULL::BIGINT AS null_placeholder
FROM source
```

### Common Types
- `::BIGINT`, `::INT`, `::INTEGER`
- `::VARCHAR(n)`, `::NVARCHAR(n)`
- `::DATETIME`, `::DATETIME2`, `::DATE`
- `::DECIMAL(p, s)`, `::NUMERIC(p, s)`
- `::BIT`, `::VARBINARY(n)`

### Variables
Access from `config.py`:

```sql
SELECT * FROM @catalog_src.@schema_src.table_name
WHERE date >= '@global_start_date'
```

### Macros
Call Python macros from `macros/`:

```sql
SELECT @convert_event_ts_to_datetime(event_ts) AS event_datetime
SELECT @generate_source_to_concept_map()
```

### Formatting
- Lowercase SQL keywords
- 2-space indentation
- Max 80 char line width
- Qualify columns with table aliases
- Run `sqlmesh format .\models` before committing

---

## Python Models

```python
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from sqlglot.expressions import to_column

columns = {
    "id": "bigint",
    "name": "varchar(100)",
    "processed_at": "datetime",
}

@model(
    name="lth_bronze.python_model",
    kind=ModelKindName.FULL,
    columns=columns,
    cron="@daily",
    depends_on=["lth_bronze.source_table"],
    audits=[
        ("not_null", {"columns": [to_column("id")]}),
        ("unique_values", {"columns": [to_column("id")]}),
    ],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    table = context.resolve_table("lth_bronze.source_table")
    df = context.fetchdf(f"SELECT * FROM {table}")
    df["processed_at"] = execution_time
    return df[list(columns.keys())]
```

### ExecutionContext Methods
- `context.resolve_table(name)` - Get physical table name
- `context.fetchdf(query)` - Execute SQL, return DataFrame
- `context.engine_adapter` - Database adapter access
- `context.variables` - Project variables

---

## External Models

Define in `external_models.yaml`:

```yaml
- name: '[catalog].[schema].[table_name]'
  columns:
    id: BIGINT
    patient_id: NUMERIC(12, 0)
    visit_date: DATETIME2
    notes: NVARCHAR(MAX)
```

---

## Macros

Define in `macros/*.py`:

```python
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

@macro()
def my_macro(evaluator: MacroEvaluator, param1: str) -> str:
    return f"UPPER({param1})"

@macro()
def before_all(evaluator: MacroEvaluator) -> list[str]:
    schema = "lth_bronze" if evaluator.this_env == "prod" else f"lth_bronze__{evaluator.this_env}"
    return [f"CREATE SCHEMA IF NOT EXISTS {schema};"]
```

---

## CLI Commands

### Development
```bash
sqlmesh plan                          # Plan changes (interactive)
sqlmesh plan --environment dev        # Plan for specific environment
sqlmesh run --environment prod        # Apply changes
sqlmesh format .\models               # Format SQL files
sqlmesh ui --mode plan                # Launch web UI
```

### Exploration
```bash
sqlmesh dag --file dag.html           # Generate DAG visualization
sqlmesh dag --model lth_bronze.person # Show model dependencies
sqlmesh evaluate lth_bronze.person    # Evaluate single model
sqlmesh fetchdf "SELECT TOP 10 ..."   # Run query
sqlmesh info                          # Show project info
```

### Testing
```bash
sqlmesh test                          # Run all tests
sqlmesh test --match "test_person"    # Run specific test
sqlmesh audit --model lth_bronze.person # Run audits
```

### Maintenance
```bash
sqlmesh diff dev:prod                 # Compare environments
sqlmesh table_diff table1 table2      # Compare table contents
sqlmesh create_external_models        # Refresh from database
sqlmesh render lth_bronze.model_name  # Check rendered SQL
sqlmesh destroy                       # Remove all objects (CAUTION)
```

---

## OMOP Standards

### Required Fields
All OMOP tables should include:
- `source_system` - Origin EHR identifier
- `org_code` - Organization code
- `last_edit_time` - Source modification timestamp
- `insert_date_time` - ETL load timestamp (`getdate()`)

### Surrogate Keys
```sql
@generate_surrogate_key(source_id, source_system)::VARBINARY(16) AS primary_key
```

### Vocabulary Mapping
```sql
SELECT
  s.source_value,
  COALESCE(v.target_concept_id, 0) AS concept_id
FROM source AS s
LEFT JOIN vcb.vocab__source_to_concept_map AS v
  ON s.source_code = v.source_code
  AND v.concept_group = 'demographics'
```

---

## Model Templates

### Source Model
```sql
MODEL (
  name src.src_system__entity,
  kind VIEW,
  cron '@daily'
);

SELECT
  column1,
  column2,
  'org_code' AS org_code,
  'source_system' AS source_system,
  last_edit_time,
  updated_at
FROM @catalog_src.@schema_src.source_table
```

### Staging Model
```sql
MODEL (
  name stg.stg__entity,
  kind FULL,
  cron '@daily'
);

WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY primary_key
      ORDER BY last_edit_time DESC
    ) AS rn
  FROM src.src_system__entity
)
SELECT
  column1::BIGINT AS column1,
  column2::VARCHAR(100) AS column2,
  org_code,
  source_system,
  last_edit_time
FROM deduplicated
WHERE rn = 1
```

### OMOP Model
```sql
MODEL (
  name lth_bronze.omop_table,
  kind FULL,
  cron '@daily'
);

SELECT
  @generate_surrogate_key(key_cols)::VARBINARY(16) AS primary_key,
  person_id::BIGINT AS person_id,
  concept_id::BIGINT AS concept_id,
  org_code,
  source_system,
  last_edit_time,
  getdate()::DATETIME AS insert_date_time
FROM vcb.vocab__entity
```

---

## Optimization

### When to Use Each Model Kind
| Scenario                     | Kind                      |
| ---------------------------- | ------------------------- |
| Source extraction            | VIEW                      |
| Complex deduplication        | FULL                      |
| Large fact tables with dates | INCREMENTAL_BY_TIME_RANGE |
| CDC/upsert patterns          | INCREMENTAL_BY_UNIQUE_KEY |
| Reference/mapping data       | SEED                      |
| Historical tracking          | SCD_TYPE_2                |

### Pipeline Analysis
1. Run `sqlmesh dag` to visualize dependencies
2. Identify bottleneck models (FULL with many dependents)
3. Convert to INCREMENTAL where appropriate
4. Push filtering upstream

---

## Environment Variables

Required in `.env`:
```
MSSQL_HOST=server_hostname
MSSQL_DATABASE=target_database
MSSQL_STATE_HOST=state_server_hostname
MSSQL_STATE_DATABASE=state_database
MSSQL_DATABASE_SOURCE=source_database
MSSQL_SCHEMA_SOURCE=source_schema
MSSQL_SCHEMA_VOCAB=vocabulary_schema
MSSQL_SCR_DB=cancer_registry_database
MSSQL_BI_DB=bi_database
```

---

## Troubleshooting

| Issue                 | Solution                             |
| --------------------- | ------------------------------------ |
| Casting errors        | Verify `::` syntax is valid for type |
| Circular dependencies | Check `sqlmesh dag`                  |
| Missing columns       | Update `external_models.yaml`        |
| Plan failures         | Run `sqlmesh info`                   |

### Debug Commands
```bash
sqlmesh render lth_bronze.model_name    # Check rendered SQL
sqlmesh plan --no-prompts --dry-run     # Validate without executing
sqlmesh --log-level DEBUG plan          # Detailed logs
```

---

## Development Workflow

1. Follow layer hierarchy: source → staging → vocab → omop
2. Use appropriate model kinds based on data characteristics
3. Add audits for data quality validation
4. Run `sqlmesh format .\models` before committing
5. Test with `sqlmesh plan --environment dev`
6. Use SQLMesh UI for interactive development: `sqlmesh ui --mode plan`
