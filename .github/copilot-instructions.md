# Copilot Instructions for LTH OMOP ETL

This document provides detailed guidance for AI-assisted development on the LTH OMOP ETL project. The project transforms data from multiple Electronic Health Record (EHR) source systems into the OMOP Common Data Model (CDM) bronze layer using **SQLMesh**.

## Project Overview

### Purpose
Transform healthcare data from various EHR systems (Flex, SwissLab, BI Referrals, Somerset Cancer Registry) into the standardized OMOP CDM format for clinical research and analytics.

### Technology Stack
- **SQLMesh** (v0.230+): Data transformation framework
- **SQL Server (MSSQL)**: Target database
- **SQLGlot**: SQL transpilation (handles `::` casting syntax)
- **Python 3.12+**: For Python models and macros
- **uv**: Package management

### Key Conventions
- Schema prefix: `lth_bronze`
- Default model kind: `VIEW`
- SQL dialect: `tsql` with case-sensitive normalization
- Datatype casting: Use `::` syntax (e.g., `column::BIGINT`)

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
│   │   ├── flex/                # Flex staging
│   │   └── swisslab/            # SwissLab staging
│   ├── vocab/                   # Vocabulary mappings (vocab_*)
│   ├── cdc/                     # Change data capture models
│   ├── external/                # External table wrappers (ext_*)
│   ├── fact_relationship/       # OMOP fact relationships
│   ├── seeds/                   # Seed model definitions
│   └── omop/                    # Final OMOP CDM tables
├── seeds/                       # CSV files for seed data
├── audits/                      # Custom data audits
└── tests/                       # Unit tests
```

---

## Model Layer Hierarchy

The pipeline follows this data flow pattern:

```
external_models.yaml → source/ (src_*) → staging/ (stg_*) → vocab/ (vocab_*) → omop/
                           ↑
                       seeds/ (mappings)
```

### Layer Descriptions

| Layer | Prefix | Kind | Purpose |
|-------|--------|------|---------|
| External | `ext__` | FULL | Wrap external tables for SQLMesh tracking |
| Source | `src_` | VIEW | Direct views on source data with minimal transformation |
| Staging | `stg_` | FULL | Clean, deduplicate, and standardize source data |
| Vocab | `vocab_` | VIEW | Apply vocabulary mappings and concept resolution |
| OMOP | (none) | FULL | Final OMOP CDM tables |
| Seeds | Various | SEED | Static mapping/reference data |

---

## SQLMesh Model Configuration

### MODEL Block Properties

Every SQL model must start with a `MODEL` block defining metadata:

```sql
MODEL (
  name lth_bronze.model_name,       -- Required: Fully qualified name
  kind FULL,                         -- Required: FULL, VIEW, INCREMENTAL_*, SEED
  cron '@daily',                     -- Schedule (default from config)
  description 'Model description',   -- Documentation
  owner 'LTH DST',                   -- Optional: Override default
  depends_on ['lth_bronze.other'],   -- Optional: Explicit dependencies
  grain [column1, column2],          -- Optional: Primary key columns
  audits [                           -- Optional: Data quality checks
    ('not_null', {'columns': ['id']})
  ]
);
```

### Model Kinds Reference

#### FULL (Default for staging/OMOP)
Complete table rebuild each run. Use for:
- Lookup tables and dimensions
- Tables where deduplication logic requires full context
- Small to medium tables with complex transformations

```sql
MODEL (
  name lth_bronze.person,
  kind FULL,
  cron '@daily'
);
```

#### VIEW (Default)
Creates a database view. Use for:
- Source layer models
- Vocabulary mapping layers
- Virtual transformations without storage

```sql
MODEL (
  name lth_bronze.src_flex__person,
  kind VIEW,
  cron '@daily'
);
```

#### INCREMENTAL_BY_TIME_RANGE
Appends data based on time column. Use for:
- Time-series data
- Event logs
- Large fact tables with date partitioning

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

#### INCREMENTAL_BY_UNIQUE_KEY
Upserts based on unique key. Use for:
- Change data capture patterns
- Slowly changing dimensions (Type 1)
- Tables with natural keys

```sql
MODEL (
  name lth_bronze.cdc_dimension,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key [id],
    when_matched WHEN MATCHED THEN UPDATE SET target.col = source.col
  ),
  cron '@daily'
);
```

#### SEED
Loads from CSV files. Use for:
- Static mapping tables
- Reference data
- Vocabulary mappings

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
Use `::` syntax for SQL Server datatypes. SQLMesh uses SQLGlot to transpile these:

```sql
SELECT
  id::BIGINT AS id,
  name::VARCHAR(100) AS name,
  amount::DECIMAL(18, 2) AS amount,
  created_at::DATETIME AS created_at,
  some_date::DATE AS some_date,
  optional_field::VARCHAR(50) AS optional_field,  -- Can be NULL
  NULL::BIGINT AS null_placeholder                -- Explicit NULL with type
FROM source
```

### Common SQL Server Types
| SQLMesh Syntax | SQL Server Type |
|----------------|-----------------|
| `::BIGINT` | `BIGINT` |
| `::INT` or `::INTEGER` | `INT` |
| `::VARCHAR(n)` | `VARCHAR(n)` |
| `::NVARCHAR(n)` | `NVARCHAR(n)` |
| `::DATETIME` | `DATETIME` |
| `::DATETIME2` | `DATETIME2` |
| `::DATE` | `DATE` |
| `::DECIMAL(p, s)` | `DECIMAL(p, s)` |
| `::NUMERIC(p, s)` | `NUMERIC(p, s)` |
| `::BIT` | `BIT` |
| `::VARBINARY(n)` | `VARBINARY(n)` |

### Using Variables
Access project variables defined in `config.py`:

```sql
-- Reference source catalog/schema
SELECT * FROM @catalog_src.@schema_src.table_name

-- Use date variables
WHERE date >= '@global_start_date'
```

### Using Macros
Call Python macros defined in `macros/`:

```sql
-- Simple macro call
SELECT @convert_event_ts_to_datetime(event_ts) AS event_datetime

-- Macro that generates SQL
SELECT @generate_source_to_concept_map()
```

### SQL Formatting Guidelines
- Use lowercase for SQL keywords (enforced by `sqlmesh format`)
- Indent with 2 spaces
- Maximum line width of 80 characters
- Use trailing commas in SELECT lists
- Qualify all column references with table aliases

```sql
MODEL (
  name lth_bronze.example,
  kind FULL
);

SELECT
  t.column_one::BIGINT AS column_one,
  t.column_two::VARCHAR(50) AS column_two,
  o.related_value::INTEGER AS related_value
FROM lth_bronze.source_table AS t
LEFT JOIN lth_bronze.other_table AS o
  ON t.id = o.source_id
WHERE
  t.is_active = 1
  AND o.status IN ('active', 'pending')
```

---

## Python Models

Use Python models when SQL is insufficient (complex transformations, external APIs, custom logic):

```python
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from sqlglot.expressions import to_column

# Define columns with SQLMesh-compatible types
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
    # Resolve table to physical name
    table = context.resolve_table("lth_bronze.source_table")

    # Fetch data as DataFrame
    df = context.fetchdf(f"SELECT * FROM {table}")

    # Transform data
    df["processed_at"] = execution_time

    # Return with correct column order
    return df[list(columns.keys())]
```

### ExecutionContext Methods
| Method | Purpose |
|--------|---------|
| `context.resolve_table(name)` | Get physical table name for model |
| `context.fetchdf(query)` | Execute SQL and return DataFrame |
| `context.engine_adapter` | Access underlying database adapter |
| `context.variables` | Access project variables |

---

## External Models

External models define source tables not managed by SQLMesh. Define in `external_models.yaml`:

```yaml
- name: '[catalog].[schema].[table_name]'
  columns:
    id: BIGINT
    patient_id: NUMERIC(12, 0)
    visit_date: DATETIME2
    notes: NVARCHAR(MAX)
    amount: DECIMAL(18, 2)
```

Key points:
- Use bracket notation for catalog.schema.table
- Define all columns with SQL Server types
- These are referenced in models without needing source definitions
- Run `sqlmesh create_external_models` to auto-generate from database

---

## Macros

### Creating Macros
Define in `macros/*.py`:

```python
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

@macro()
def my_macro(evaluator: MacroEvaluator, param1: str) -> str:
    """Return SQL string to inject into model."""
    return f"UPPER({param1})"

@macro()
def before_all(evaluator: MacroEvaluator) -> list[str]:
    """Return list of SQL statements for pre-execution."""
    schema = (
        "lth_bronze"
        if evaluator.this_env == "prod"
        else f"lth_bronze__{evaluator.this_env}"
    )
    return [f"CREATE SCHEMA IF NOT EXISTS {schema};"]
```

### Evaluator Properties
| Property | Description |
|----------|-------------|
| `evaluator.this_env` | Current environment name |
| `evaluator.gateway` | Current gateway configuration |
| `evaluator.variables` | Project variables |

---

## Audits (Data Quality)

### Built-in Audits
```python
audits=[
    ("not_null", {"columns": [to_column("id"), to_column("name")]}),
    ("unique_values", {"columns": [to_column("id")]}),
    ("accepted_values", {"column": to_column("status"), "values": ["active", "inactive"]}),
    ("number_of_rows", {"threshold": 1}),
]
```

### Custom SQL Audits
Create in `audits/*.sql`:

```sql
AUDIT (
  name custom_validation,
  dialect tsql
);

SELECT *
FROM @this_model
WHERE amount < 0
```

---

## CLI Commands

### Development Workflow
```powershell
# Plan changes (interactive)
sqlmesh plan

# Plan for specific environment
sqlmesh plan --environment dev

# Apply changes without prompts
sqlmesh run --environment prod

# Format SQL files
sqlmesh format .\models

# Launch web UI in plan mode
sqlmesh ui --mode plan
```

### Exploration Commands
```powershell
# Generate DAG visualization
sqlmesh dag --file dag.html

# Show model dependencies
sqlmesh dag --model lth_bronze.person

# Evaluate single model
sqlmesh evaluate lth_bronze.person

# Run query
sqlmesh fetchdf "SELECT TOP 10 * FROM lth_bronze.person"

# Show project info
sqlmesh info
```

### Testing & Auditing
```powershell
# Run all tests
sqlmesh test

# Run specific test
sqlmesh test --match "test_person"

# Run audits
sqlmesh audit --model lth_bronze.person
```

### Maintenance
```powershell
# Compare environments
sqlmesh diff dev:prod

# Compare table contents
sqlmesh table_diff lth_bronze.person lth_bronze__dev.person

# Refresh external models from database
sqlmesh create_external_models

# Destroy all SQLMesh objects (CAUTION)
sqlmesh destroy
```

---

## OMOP CDM Specific Guidelines

### Person ID Generation
Use consistent surrogate key generation:

```sql
SELECT
  @generate_surrogate_key(source_id, source_system)::VARBINARY(16) AS person_id
FROM source
```

### Standard OMOP Fields
All OMOP tables should include:
- `source_system`: Origin EHR system identifier
- `org_code`: Organization code
- `last_edit_time`: Last modification timestamp from source
- `insert_date_time`: ETL load timestamp (`getdate()`)

### Vocabulary Mappings
Use the `vocab__source_to_concept_map` model for standardized concept resolution:

```sql
SELECT
  s.source_value,
  COALESCE(v.target_concept_id, 0) AS concept_id
FROM source AS s
LEFT JOIN lth_bronze.vocab__source_to_concept_map AS v
  ON s.source_code = v.source_code
  AND v.concept_group = 'demographics'
```

---

## Optimization Guidelines

### Pipeline Optimization
When asked to optimize:
1. Run `sqlmesh dag` to visualize dependencies
2. Identify bottleneck models (FULL tables with many dependents)
3. Consider converting to INCREMENTAL where appropriate
4. Consolidate redundant transformations
5. Push filtering upstream to reduce data volume

### Model Selection Criteria
| Scenario | Recommended Kind |
|----------|------------------|
| Source data extraction | VIEW |
| Complex deduplication | FULL |
| Large fact tables with dates | INCREMENTAL_BY_TIME_RANGE |
| CDC/upsert patterns | INCREMENTAL_BY_UNIQUE_KEY |
| Reference/mapping data | SEED |
| Historical tracking | SCD_TYPE_2 |

### Query Performance
- Add appropriate indexes in post_statements for FULL models
- Use column pruning (select only needed columns)
- Filter early in the pipeline
- Avoid SELECT * in production models

---

## Environment Variables

Required in `.env` or environment:

```env
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

## Common Patterns

### Source Model Template
```sql
MODEL (
  name lth_bronze.src_system__entity,
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

### Staging Model Template
```sql
MODEL (
  name lth_bronze.stg__entity,
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
  FROM lth_bronze.src_system__entity
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

### OMOP Model Template
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
  -- Standard OMOP columns
  org_code,
  source_system,
  last_edit_time,
  getdate()::DATETIME AS insert_date_time
FROM lth_bronze.vocab__entity
```

---

## Troubleshooting

### Common Issues

1. **Casting errors**: Ensure `::` syntax is valid for target type
2. **Circular dependencies**: Check DAG with `sqlmesh dag`
3. **Missing columns**: Verify external_models.yaml matches source schema
4. **Plan failures**: Check `sqlmesh info` for configuration issues

### Debug Commands
```powershell
# Check model SQL
sqlmesh render lth_bronze.model_name

# Validate without executing
sqlmesh plan --no-prompts --dry-run

# Show detailed logs
sqlmesh --log-level DEBUG plan
```

---

## VS Code Integration

### Recommended Workflow
1. Use Simple Browser with SQLMesh UI: `http://127.0.0.1:8000`
2. Run `SQLMesh UI Plan` task for interactive development
3. Use `sqlmesh format .\models` before committing

### Tasks
- `SQLMesh UI Plan`: Launch interactive UI for plan mode development

---

## Contributing Guidelines

1. Follow the layer hierarchy (source → staging → vocab → omop)
2. Use appropriate model kinds based on data characteristics
3. Include descriptive comments for complex transformations
4. Add audits for data quality validation
5. Run `sqlmesh format` before committing
6. Test changes with `sqlmesh plan --environment dev`
