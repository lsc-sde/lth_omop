# LTH OMOP ETL - Comprehensive Optimization Plan

**Date:** 2026-02-14  
**Version:** 1.1  
**Status:** Updated for Schema Separation (PR #24)  
**Last Updated:** 2026-02-16

---

> **📝 Note on Schema Separation (PR #24)**  
> As of PR #24, models have been reorganized into separate schemas for better organization:
> - **`src.*`** - Source models (e.g., `src.src_flex__person`)
> - **`stg.*`** - Staging models (e.g., `stg.stg__person`)
> - **`vcb.*`** - Vocabulary models (e.g., `vcb.vocab__person`)
> - **`cdm.*`** - OMOP CDM models (e.g., `cdm.person`)
> - **`ext.*`** - External models (e.g., `ext.ext__postcodes`)
> 
> This document has been updated to reflect the new schema structure.

---

## Executive Summary

This document provides a detailed analysis and optimization plan for the LTH OMOP ETL SQLMesh project. The project transforms healthcare data from multiple EHR source systems (Flex, SwissLab, BI, Endoscopy) into the OMOP Common Data Model bronze layer.

### Current State
- **Total Models:** 126 SQL models + seeds
- **Architecture:** 4-layer pipeline (source → staging → vocab → OMOP)
- **Source Systems:** Flex (primary), SwissLab, BI, Endoscopy
- **OMOP Tables:** 16 final tables (13 core, 3 supporting)
- **Model Kinds:** Primarily FULL refresh, with 1 INCREMENTAL_BY_TIME_RANGE (measurement)

### Key Findings
1. **Excellent Architecture**: Clear layer separation with well-defined responsibilities
2. **Optimization Opportunities**: Model consolidation, incremental strategies, query optimization
3. **Performance Bottlenecks**: Large FULL refresh models, complex vocab joins
4. **Maintenance Overhead**: Fragmented source models for similar entities

---

## 1. Architecture Analysis

### 1.1 Layer Structure

```
models/
├── source/              # 33 VIEW models (raw data extraction)
│   ├── flex/           # 21 models - EHR system
│   ├── swisslab/       # 2 models - Lab system  
│   ├── bi/             # 2 models - BI system
│   └── endoscopy/      # 8 models - GI procedures
├── staging/            # 23 FULL models (transformation & deduplication)
│   ├── flex/           # 14 flex-specific staging
│   ├── swisslab/       # 2 swisslab staging
│   ├── bi/             # 1 bi staging
│   └── common/         # 6 system-agnostic staging
├── vocab/              # 16 VIEW models (vocabulary mapping)
│   └── vocab__*        # Apply OMOP concept mappings
└── omop/               # 16 models (final OMOP CDM tables)
    └── {table_name}    # 15 FULL, 1 INCREMENTAL_BY_TIME_RANGE
```

### 1.2 Data Flow Pattern

```
External Sources → Source Views → Staging Tables → Vocab Views → CDM Tables
     (VIEWs)         (VIEWs)         (FULL)         (VIEWs)      (FULL/INCR)
       src.*          src.*            stg.*          vcb.*         cdm.*
```

**Note:** As of PR #24, models are organized into separate schemas:
- **src**: Source models (`src.src_flex__*`, `src.src_sl__*`, etc.)
- **stg**: Staging models (`stg.stg__*`, `stg.stg_flex__*`, etc.)
- **vcb**: Vocabulary models (`vcb.vocab__*`)
- **cdm**: OMOP CDM models (`cdm.person`, `cdm.measurement`, etc.)
- **ext**: External models (`ext.ext__*`)

### 1.3 Current Model Kinds Distribution

| Layer | FULL | VIEW | INCREMENTAL | SEED | Total |
|-------|------|------|-------------|------|-------|
| Source | 0 | 33 | 0 | 0 | 33 |
| Staging | 23 | 0 | 0 | 0 | 23 |
| Vocab | 1 | 15 | 0 | 0 | 16 |
| OMOP | 14 | 0 | 1 | 0 | 15 |
| Seeds | 0 | 0 | 0 | 28 | 28 |
| **Total** | **38** | **48** | **1** | **28** | **115** |

---

## 2. OMOP Table Dependency Analysis

### 2.1 Dependency Summary

| OMOP Table | Model Kind | Direct Dependencies | Vocab Models | OMOP Dependencies |
|------------|-----------|---------------------|--------------|-------------------|
| person | FULL | 4 | vocab__person | care_site, provider |
| visit_occurrence | FULL | 3 | vocab__visit_occurrence | care_site, provider |
| visit_detail | FULL | 2 | vocab__visit_detail | care_site |
| care_site | FULL | 2 | vocab__care_site | location |
| provider | FULL | 2 | vocab__provider | care_site |
| condition_occurrence | FULL | 2 | vocab__condition_occurrence | (vocab__provider) |
| procedure_occurrence | FULL | 1 | vocab__procedure_occurrence | - |
| drug_exposure | FULL | 2 | vocab__drug_exposure | provider |
| device_exposure | FULL | 2 | vocab__device_exposure, vocab__device | - |
| **measurement** | **INCR_TIME** | 2 | vocab__measurement | provider |
| observation | FULL | 2 | vocab__observation | provider |
| specimen | FULL | 1 | vocab__specimen | - |
| death | FULL | 1 | vocab__death | - |
| location | Python | - | - | - |
| source_to_concept_map | FULL | 2 | vocab__source_to_concept_map_raw | - |

### 2.2 Critical Path Analysis

**Dimension Tables (Load First):**
1. location → care_site → provider → person
2. source_to_concept_map (vocabulary foundation)

**Fact Tables (Load After Dimensions):**
3. visit_occurrence → visit_detail
4. condition_occurrence, procedure_occurrence, drug_exposure
5. device_exposure, measurement, observation, specimen
6. death
7. fact_relationship (depends on multiple fact tables)

---

## 3. Performance Analysis

### 3.1 Model Complexity Metrics

#### Vocab Layer Complexity
| Model | Lines | CTEs | JOINs | Complexity |
|-------|-------|------|-------|------------|
| vocab__observation | 196 | 3 | 9 | HIGH |
| vocab__visit_occurrence | 104 | 2 | 2 | MEDIUM |
| vocab__measurement | 99 | 0 | 0 | LOW |
| vocab__condition_occurrence | 97 | 4 | 8 | HIGH |
| vocab__device_exposure | 88 | 2 | 2 | MEDIUM |
| vocab__procedure_occurrence | 84 | 4 | 4 | MEDIUM |
| vocab__care_site | 74 | 1 | 0 | LOW |
| vocab__drug_exposure | 61 | 1 | 3 | MEDIUM |
| vocab__visit_detail | 54 | 1 | 1 | LOW |
| vocab__person | 52 | 0 | 2 | LOW |

**Average:** 67 lines, 1.2 CTEs, 2.1 JOINs per vocab model

#### Staging Layer Complexity
| Model | Lines | Type | Notes |
|-------|-------|------|-------|
| stg__master_patient_index | 167 | FULL | Master patient linking |
| stg_sl__bacteriology | 118 | FULL | Lab data union + dedup |
| stg_flex__procedure_occurrence | 110 | FULL | Multiple procedure sources |
| stg_flex__facility_transfer | 104 | FULL | Visit consolidation |
| stg_flex__condition_occurrence | 86 | FULL | Diagnosis aggregation |
| stg_flex__visit_detail | 84 | FULL | Visit detail dedup |
| stg__person | 72 | FULL | Person deduplication |

### 3.2 Current Bottlenecks

1. **Large FULL Refresh Models**
   - `stg__master_patient_index` (167 lines): Full patient matching every run
   - `stg_sl__bacteriology` (118 lines): Union of live + archive data
   - `stg_flex__facility_transfer` (104 lines): Complex visit consolidation

2. **Measurement Table Performance**
   - Already optimized with INCREMENTAL_BY_TIME_RANGE
   - Good batch_size (30 days) and concurrency (4)
   - Template for other fact tables

3. **Vocab Layer JOINs**
   - `vocab__observation`: 9 JOINs to OMOP vocabulary
   - `vocab__condition_occurrence`: 8 JOINs for concept mapping
   - Multiple JOINs to `vocab__source_to_concept_map`

---

## 4. Optimization Recommendations

### 4.1 Model Consolidation Opportunities

#### Priority 1: Source Layer Consolidation

**Problem:** Multiple similar source models with minimal transformation

**Current State:**
- 3 care_site models: `src_flex__care_site_gp`, `src_flex__care_site_ip`, `src_flex__care_site_op`
- 3 employee models: `src_flex__emp_type`, `src_flex__emp_provider`, `src_flex__emp_consultant`
- 2 diagnosis models: `src_flex__ae_diagnosis`, `src_flex__vtg_diagnosis`
- 2 procedure models: `src_flex__ae_procedures`, `src_flex__vtg_procedure`
- 2 device models: `src_flex__implant_devices`, `src_flex__cathether_devices`

**Recommendation:**
Consolidate into fewer models with source type discriminator:

```sql
-- New: src_flex__care_site (replaces 3 models)
SELECT *, 'GP' AS care_site_type FROM src_flex__care_site_gp
UNION ALL
SELECT *, 'IP' AS care_site_type FROM src_flex__care_site_ip  
UNION ALL
SELECT *, 'OP' AS care_site_type FROM src_flex__care_site_op

-- New: src_flex__diagnosis (replaces 2 models)
SELECT *, 'AE' AS diagnosis_source FROM src_flex__ae_diagnosis
UNION ALL
SELECT *, 'VTG' AS diagnosis_source FROM src_flex__vtg_diagnosis
```

**Benefits:**
- Reduces model count by ~40% in source layer
- Simplifies downstream staging dependencies
- Easier maintenance

**Impact:** LOW - Downstream models already handle multiple sources

#### Priority 2: Staging Layer Optimization

**Current Issues:**
- `stg__result` is INCREMENTAL but most staging is FULL
- Large staging models rebuild entirely daily
- No change data capture for dimension tables

**Recommendations:**

1. **Convert Large Fact Staging to INCREMENTAL_BY_TIME_RANGE:**

Models to convert:
- `stg__result` (already done ✓)
- `stg_flex__result` 
- `stg_bi__referrals`
- `stg_sl__bacteriology` (if has updated_at)

2. **Consider SCD Type 2 for Slowly Changing Dimensions:**

Candidates:
- `stg__person` (demographics changes)
- `stg__provider` (provider changes)
- `stg_flex__care_site` (location changes)

3. **Keep Master Patient Index as FULL:**
- Complex patient matching logic requires full context
- Not a performance bottleneck (dimension table)

#### Priority 3: Vocab Layer Optimization

**Problem:** Repeated JOINs to `vocab__source_to_concept_map` across all vocab models

**Current Pattern:**
```sql
-- Repeated in every vocab__ model
LEFT JOIN (
  SELECT source_code, target_concept_id
  FROM vcb.vocab__source_to_concept_map
  WHERE concept_group = 'result'
) AS cm ON r.source_code = cm.source_code
```

**Recommendation:**
Create materialized concept mapping views by domain:

```sql
-- New: vcb.vocab__concept_map_measurement (FULL)
SELECT * FROM vcb.vocab__source_to_concept_map
WHERE concept_group IN ('result', 'units', 'decoded')

-- New: vcb.vocab__concept_map_condition (FULL)  
SELECT * FROM vcb.vocab__source_to_concept_map
WHERE concept_group IN ('diagnosis', 'condition')
```

**Benefits:**
- Pre-filtered concept maps reduce JOIN complexity
- Faster vocab query execution
- Clearer vocab model logic

**Trade-off:** Additional models but simpler queries

#### Priority 4: OMOP Layer Incremental Strategy

**Current State:** Only `measurement` uses incremental loading

**High-Value Candidates for INCREMENTAL_BY_TIME_RANGE:**

1. **observation** (similar to measurement)
   - Large fact table with `updated_at` column
   - Time-series data
   - **Expected Benefit:** 70-90% faster refresh

2. **condition_occurrence**
   - Has `updated_at` from staging layer
   - Event-based data
   - **Expected Benefit:** 60-80% faster refresh

3. **procedure_occurrence**
   - Has `updated_at` tracking
   - Event-based data
   - **Expected Benefit:** 60-80% faster refresh

4. **drug_exposure**
   - Has `updated_at` tracking
   - Large volume table
   - **Expected Benefit:** 70-90% faster refresh

5. **device_exposure**
   - Has `updated_at` tracking
   - Event-based data
   - **Expected Benefit:** 50-70% faster refresh

**Keep as FULL:**
- Dimension tables (person, provider, care_site, location)
- Small fact tables (specimen, death)
- Tables without reliable updated_at (visit_occurrence, visit_detail)

### 4.2 SQL Query Optimizations

#### General Patterns to Apply

1. **Remove Redundant DISTINCT**
```sql
-- Before
SELECT DISTINCT person_id, ... FROM table
WHERE already_unique_key = x

-- After  
SELECT person_id, ... FROM table
WHERE already_unique_key = x
```

2. **Push Down Filters**
```sql
-- Before
WITH all_data AS (SELECT * FROM large_table)
SELECT * FROM all_data WHERE date > '2020-01-01'

-- After
WITH filtered_data AS (
  SELECT * FROM large_table 
  WHERE date > '2020-01-01'
)
SELECT * FROM filtered_data
```

3. **Avoid Subqueries in SELECT**
```sql
-- Before
SELECT 
  id,
  (SELECT name FROM lookup WHERE id = t.lookup_id) AS name
FROM table t

-- After
SELECT t.id, l.name
FROM table t
LEFT JOIN lookup l ON t.lookup_id = l.id
```

4. **Use CROSS APPLY Instead of Multiple Subqueries**
```sql
-- Before (multiple correlated subqueries)
SELECT 
  t.id,
  (SELECT MAX(date) FROM events WHERE id = t.id) AS max_date,
  (SELECT COUNT(*) FROM events WHERE id = t.id) AS cnt
FROM table t

-- After (single CROSS APPLY)
SELECT t.id, e.max_date, e.cnt
FROM table t
CROSS APPLY (
  SELECT MAX(date) AS max_date, COUNT(*) AS cnt
  FROM events WHERE id = t.id
) e
```

#### Model-Specific Optimizations

**vocab__observation (196 lines, 9 JOINs):**
- **Issue:** Multiple JOINs to source_to_concept_map with different filters
- **Fix:** Create concept_group-specific CTEs at top
- **Estimated Improvement:** 30-40% faster

**vocab__condition_occurrence (97 lines, 8 JOINs):**
- **Issue:** Complex ICD10→SNOMED mapping with vocabulary JOINs
- **Fix:** Materialize concept_relationship mappings
- **Estimated Improvement:** 40-50% faster

**stg__master_patient_index (167 lines):**
- **Issue:** Complex UNION and patient linking logic
- **Keep as-is:** Logic requires full context, already optimized
- **Alternative:** Consider external patient matching service

**stg_sl__bacteriology (118 lines):**
- **Issue:** UNION ALL of live + archive data with same filtering
- **Fix:** Apply filtering before UNION
- **Estimated Improvement:** 20-30% faster

**stg_flex__facility_transfer (104 lines):**
- **Issue:** Complex visit consolidation with multiple aggregations
- **Keep as-is:** Business logic is inherently complex
- **Alternative:** Add indexes on join keys if performance issues

### 4.3 Architecture Improvements

#### Recommendation 1: Add CDC Layer (Optional)

Create a change data capture layer for large dimension tables:

```
models/cdc/
├── cdc__person.sql           # Track person changes
├── cdc__provider.sql         # Track provider changes
└── cdc__care_site.sql        # Track care site changes
```

**Benefits:**
- Faster dimension updates
- Historical tracking of changes
- Reduced load on source systems

**Complexity:** Medium - requires CDC infrastructure

#### Recommendation 2: Introduce Mart Layer (Future)

For analytics use cases, add a mart layer above OMOP:

```
models/mart/
├── mart__patient_summary.sql     # Patient 360 view
├── mart__visit_metrics.sql       # Visit analytics
└── mart__quality_measures.sql    # Clinical quality
```

**Benefits:**
- Optimized for specific analytics
- Denormalized for performance
- Business-friendly naming

**Timeline:** Phase 2 (after bronze optimization)

#### Recommendation 3: Performance Monitoring

Add audit queries to track model performance:

```sql
-- models/audits/performance/
├── audit__model_row_counts.sql
├── audit__model_execution_time.sql
└── audit__incremental_efficiency.sql
```

---

## 5. Implementation Plan

### Phase 1: Quick Wins (Week 1-2)
**Priority:** HIGH | **Risk:** LOW | **Effort:** LOW

- [x] Document current architecture
- [ ] Create optimization plan
- [ ] Apply SQL best practices to top 5 slowest queries
  - [ ] vocab__observation
  - [ ] vocab__condition_occurrence
  - [ ] stg_sl__bacteriology
  - [ ] vocab__measurement
  - [ ] vocab__visit_occurrence
- [ ] Remove redundant DISTINCT clauses
- [ ] Push down filters in CTEs

**Expected Impact:** 15-25% overall performance improvement

### Phase 2: Source Consolidation (Week 3-4)
**Priority:** MEDIUM | **Risk:** LOW | **Effort:** MEDIUM

- [ ] Consolidate flex care_site models (3→1)
- [ ] Consolidate flex employee models (3→1)
- [ ] Consolidate flex diagnosis models (2→1)
- [ ] Consolidate flex procedure models (2→1)
- [ ] Consolidate flex device models (2→1)
- [ ] Update downstream staging dependencies
- [ ] Test consolidated models

**Expected Impact:** 10 fewer models, simpler architecture

### Phase 3: Incremental Loading (Week 5-8)
**Priority:** HIGH | **Risk:** MEDIUM | **Effort:** HIGH

- [ ] Convert observation to INCREMENTAL_BY_TIME_RANGE
  - [ ] Add updated_at tracking
  - [ ] Test incremental logic
  - [ ] Backfill historical data
- [ ] Convert condition_occurrence to INCREMENTAL_BY_TIME_RANGE
- [ ] Convert procedure_occurrence to INCREMENTAL_BY_TIME_RANGE
- [ ] Convert drug_exposure to INCREMENTAL_BY_TIME_RANGE
- [ ] Convert device_exposure to INCREMENTAL_BY_TIME_RANGE
- [ ] Monitor incremental efficiency

**Expected Impact:** 60-80% faster refresh for converted tables

### Phase 4: Vocab Optimization (Week 9-10)
**Priority:** MEDIUM | **Risk:** LOW | **Effort:** MEDIUM

- [ ] Create domain-specific concept map views
  - [ ] vocab__concept_map_measurement
  - [ ] vocab__concept_map_condition
  - [ ] vocab__concept_map_procedure
  - [ ] vocab__concept_map_drug
  - [ ] vocab__concept_map_device
- [ ] Refactor vocab models to use pre-filtered maps
- [ ] Test concept mapping accuracy
- [ ] Performance benchmark

**Expected Impact:** 30-50% faster vocab layer

### Phase 5: Advanced Optimizations (Week 11-12)
**Priority:** LOW | **Risk:** MEDIUM | **Effort:** HIGH

- [ ] Evaluate CDC for dimension tables
- [ ] Add performance monitoring audits
- [ ] Create indexes on frequent join keys
- [ ] Consider partitioning strategy for large fact tables
- [ ] Documentation updates

**Expected Impact:** 10-20% additional improvement

---

## 6. Subtasks for Implementation

### 6.1 SQL Optimization Subtasks

#### Subtask 1.1: Optimize vocab__observation Query
**Assignee:** Backend Developer  
**Effort:** 2-3 hours  
**Files:** `models/vocab/vocab__observation.sql`

**Tasks:**
1. Create dedicated CTEs for each concept_group filter
2. Replace repeated source_to_concept_map subqueries with CTEs
3. Remove unnecessary DISTINCT if not needed
4. Test query performance before/after
5. Validate result accuracy

**Acceptance Criteria:**
- Query executes 30%+ faster
- Row count matches original model
- All concept mappings preserved

---

#### Subtask 1.2: Optimize vocab__condition_occurrence Query
**Assignee:** Backend Developer  
**Effort:** 2-3 hours  
**Files:** `models/vocab/vocab__condition_occurrence.sql`

**Tasks:**
1. Materialize ICD10→SNOMED mappings in CTE
2. Reduce JOINs to vocabulary tables
3. Push concept filtering earlier in pipeline
4. Test query performance
5. Validate concept mapping accuracy

**Acceptance Criteria:**
- Query executes 40%+ faster
- ICD10 to SNOMED mappings correct
- No missing concept IDs

---

#### Subtask 1.3: Optimize stg_sl__bacteriology Query
**Assignee:** Backend Developer  
**Effort:** 1-2 hours  
**Files:** `models/staging/swisslab/stg_sl__bacteriology.sql`

**Tasks:**
1. Apply invalid_results filter before UNION
2. Move common filters to CTE
3. Test with production data volumes
4. Verify deduplication logic

**Acceptance Criteria:**
- Query executes 20%+ faster
- Row counts match original
- No duplicate records

---

#### Subtask 1.4: Remove Redundant DISTINCT Clauses
**Assignee:** Backend Developer  
**Effort:** 2-3 hours  
**Files:** Multiple staging and vocab models

**Tasks:**
1. Identify DISTINCT usage across all models
2. Analyze if DISTINCT is necessary (check for natural keys)
3. Remove where unnecessary
4. Add comments explaining why DISTINCT needed where kept
5. Run tests to verify row counts

**Acceptance Criteria:**
- Document DISTINCT usage review
- Remove 30%+ of unnecessary DISTINCTs
- No change in output row counts

---

### 6.2 Model Consolidation Subtasks

#### Subtask 2.1: Consolidate Flex Care Site Models
**Assignee:** Data Engineer  
**Effort:** 4-6 hours  
**Files:** 
- `models/source/flex/src_flex__care_site_gp.sql`
- `models/source/flex/src_flex__care_site_ip.sql`
- `models/source/flex/src_flex__care_site_op.sql`
- `models/staging/flex/stg_flex__care_site.sql` (update)

**Tasks:**
1. Create new `src_flex__care_site.sql` with UNION ALL
2. Add `care_site_type` discriminator column ('GP', 'IP', 'OP')
3. Ensure all columns from 3 models are included
4. Update `stg_flex__care_site` to reference new model
5. Test downstream dependencies (vocab__care_site, care_site)
6. Remove old 3 models after validation

**Acceptance Criteria:**
- Single source model replaces 3 models
- All care site types preserved
- Downstream models work correctly
- Row counts match sum of originals

---

#### Subtask 2.2: Consolidate Flex Employee Models
**Assignee:** Data Engineer  
**Effort:** 4-6 hours  
**Files:**
- `models/source/flex/src_flex__emp_type.sql`
- `models/source/flex/src_flex__emp_provider.sql`
- `models/source/flex/src_flex__emp_consultant.sql`
- `models/staging/flex/stg_flex__provider.sql` (update)

**Tasks:**
1. Analyze if these can be consolidated or should remain separate
2. If consolidating, create `src_flex__employee.sql`
3. Add `employee_type` discriminator
4. Update staging provider references
5. Test provider assignment logic
6. Remove old models after validation

**Acceptance Criteria:**
- Provider data integrity maintained
- No missing provider assignments
- All provider types preserved

---

#### Subtask 2.3: Consolidate Flex Diagnosis Models
**Assignee:** Data Engineer  
**Effort:** 3-4 hours  
**Files:**
- `models/source/flex/src_flex__ae_diagnosis.sql`
- `models/source/flex/src_flex__vtg_diagnosis.sql`
- `models/staging/flex/stg_flex__condition_occurrence.sql` (update)

**Tasks:**
1. Create `src_flex__diagnosis.sql` with UNION ALL
2. Add `diagnosis_source` column ('AE', 'VTG')
3. Ensure all diagnosis codes preserved
4. Update condition_occurrence staging
5. Test diagnosis mappings
6. Remove old models

**Acceptance Criteria:**
- All diagnosis codes preserved
- Source tracking maintained
- Condition mappings accurate

---

#### Subtask 2.4: Consolidate Flex Procedure Models
**Assignee:** Data Engineer  
**Effort:** 3-4 hours  
**Files:**
- `models/source/flex/src_flex__ae_procedures.sql`
- `models/source/flex/src_flex__vtg_procedure.sql`
- `models/staging/flex/stg_flex__procedure_occurrence.sql` (update)

**Tasks:**
1. Create `src_flex__procedure.sql` with UNION ALL
2. Add `procedure_source` column ('AE', 'VTG')
3. Preserve all procedure codes
4. Update procedure_occurrence staging
5. Test procedure mappings
6. Remove old models

**Acceptance Criteria:**
- All procedure codes preserved
- Source attribution maintained
- Procedure mappings correct

---

#### Subtask 2.5: Consolidate Flex Device Models
**Assignee:** Data Engineer  
**Effort:** 2-3 hours  
**Files:**
- `models/source/flex/src_flex__implant_devices.sql`
- `models/source/flex/src_flex__cathether_devices.sql`
- `models/staging/flex/stg_flex__devices.sql` (update)

**Tasks:**
1. Create `src_flex__device.sql` with UNION ALL
2. Add `device_type` column ('implant', 'catheter')
3. Ensure all device records included
4. Update device_exposure staging
5. Test device mappings
6. Remove old models

**Acceptance Criteria:**
- All devices tracked
- Device type classification correct
- Device exposure records accurate

---

### 6.3 Incremental Loading Subtasks

#### Subtask 3.1: Convert observation to INCREMENTAL_BY_TIME_RANGE
**Assignee:** Senior Data Engineer  
**Effort:** 8-12 hours  
**Priority:** HIGH  
**Files:**
- `models/omop/observation.sql`
- `models/vocab/vocab__observation.sql`

**Tasks:**
1. Verify `updated_at` column exists and is reliable
2. Change model kind to INCREMENTAL_BY_TIME_RANGE
3. Add `WHERE updated_at BETWEEN @start_ds AND @end_ds`
4. Configure batch_size (start with 30 days)
5. Configure batch_concurrency (start with 4)
6. Test incremental load with sample date range
7. Backfill historical data in batches
8. Monitor incremental efficiency
9. Document configuration choices

**Configuration:**
```sql
MODEL (
  name cdm.observation,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at,
    batch_size 30,
    batch_concurrency 4
  )
)
```

**Acceptance Criteria:**
- Incremental loads match full refresh results
- No duplicate records
- Performance improvement >60%
- Idempotent (can re-run same date range)

---

#### Subtask 3.2: Convert condition_occurrence to INCREMENTAL_BY_TIME_RANGE
**Assignee:** Senior Data Engineer  
**Effort:** 6-8 hours  
**Files:**
- `models/omop/condition_occurrence.sql`
- `models/vocab/vocab__condition_occurrence.sql`

**Tasks:**
1. Verify updated_at tracking
2. Convert to INCREMENTAL_BY_TIME_RANGE
3. Add time filter predicate
4. Test incremental behavior
5. Backfill historical data
6. Performance benchmark

**Acceptance Criteria:**
- Incremental loads correct
- Performance improvement >60%
- No missing conditions

---

#### Subtask 3.3: Convert procedure_occurrence to INCREMENTAL_BY_TIME_RANGE
**Assignee:** Senior Data Engineer  
**Effort:** 6-8 hours  
**Files:**
- `models/omop/procedure_occurrence.sql`
- `models/vocab/vocab__procedure_occurrence.sql`

**Tasks:**
1. Verify updated_at tracking
2. Convert to INCREMENTAL_BY_TIME_RANGE
3. Add time filter
4. Test and backfill
5. Benchmark performance

**Acceptance Criteria:**
- Incremental loads accurate
- Performance improvement >60%
- All procedures captured

---

#### Subtask 3.4: Convert drug_exposure to INCREMENTAL_BY_TIME_RANGE
**Assignee:** Senior Data Engineer  
**Effort:** 6-8 hours  
**Files:**
- `models/omop/drug_exposure.sql`
- `models/vocab/vocab__drug_exposure.sql`

**Tasks:**
1. Verify updated_at column
2. Convert to INCREMENTAL_BY_TIME_RANGE
3. Add time filter
4. Test and backfill
5. Benchmark

**Acceptance Criteria:**
- Incremental loads correct
- Performance improvement >70%
- Drug records complete

---

#### Subtask 3.5: Convert device_exposure to INCREMENTAL_BY_TIME_RANGE
**Assignee:** Senior Data Engineer  
**Effort:** 4-6 hours  
**Files:**
- `models/omop/device_exposure.sql`
- `models/vocab/vocab__device_exposure.sql`

**Tasks:**
1. Verify updated_at tracking
2. Convert to INCREMENTAL_BY_TIME_RANGE
3. Add time filter
4. Test and backfill
5. Benchmark

**Acceptance Criteria:**
- Incremental loads accurate
- Performance improvement >50%
- Device exposure complete

---

### 6.4 Vocab Optimization Subtasks

#### Subtask 4.1: Create Domain-Specific Concept Map Views
**Assignee:** Data Engineer  
**Effort:** 4-6 hours  
**Files:** New files in `models/vocab/`

**Tasks:**
1. Create `vocab__concept_map_measurement.sql`
   - Filter: concept_group IN ('result', 'units', 'decoded', 'bacteria_*')
2. Create `vocab__concept_map_condition.sql`
   - Filter: concept_group IN ('diagnosis', 'condition')
3. Create `vocab__concept_map_procedure.sql`
   - Filter: concept_group IN ('procedure')
4. Create `vocab__concept_map_drug.sql`
   - Filter: concept_group IN ('drug', 'drug_routes')
5. Create `vocab__concept_map_device.sql`
   - Filter: concept_group IN ('device')
6. All models should be kind FULL
7. Test that all concept_groups covered

**Acceptance Criteria:**
- 5 new concept map views created
- No missing concept_groups
- Views materialize correctly

---

#### Subtask 4.2: Refactor vocab Models to Use Pre-filtered Maps
**Assignee:** Data Engineer  
**Effort:** 6-8 hours  
**Files:** All `models/vocab/vocab__*.sql` files

**Tasks:**
1. Update vocab__measurement to use vocab__concept_map_measurement
2. Update vocab__condition_occurrence to use vocab__concept_map_condition
3. Update vocab__procedure_occurrence to use vocab__concept_map_procedure
4. Update vocab__drug_exposure to use vocab__concept_map_drug
5. Update vocab__device_exposure to use vocab__concept_map_device
6. Remove inline concept_group filters
7. Test all vocab models for accuracy
8. Performance benchmark each model

**Acceptance Criteria:**
- All vocab models refactored
- Concept mappings identical to before
- Performance improvement 30-50%

---

### 6.5 Testing and Validation Subtasks

#### Subtask 5.1: Create Data Quality Tests
**Assignee:** QA Engineer  
**Effort:** 8-10 hours  
**Files:** `tests/` directory

**Tasks:**
1. Row count validation tests for all OMOP tables
2. Concept mapping accuracy tests
3. Foreign key integrity tests
4. Incremental idempotency tests
5. Data lineage tests
6. Automated regression test suite

**Acceptance Criteria:**
- Comprehensive test coverage
- All tests passing
- Automated CI/CD integration

---

#### Subtask 5.2: Performance Benchmarking
**Assignee:** Data Engineer  
**Effort:** 4-6 hours  
**Files:** `docs/BENCHMARKS.md`

**Tasks:**
1. Baseline current performance metrics
2. Benchmark after each optimization phase
3. Document execution times per model
4. Track row counts and data volumes
5. Monitor resource utilization
6. Create performance dashboard

**Deliverables:**
- Performance comparison report
- Optimization ROI analysis
- Recommendations for further tuning

---

## 7. Risk Assessment

### High Risk Items
1. **Incremental Loading Conversion**
   - **Risk:** Data loss or duplication
   - **Mitigation:** Thorough testing, parallel runs, rollback plan
   - **Contingency:** Keep FULL refresh as fallback

2. **Source Model Consolidation**
   - **Risk:** Breaking downstream dependencies
   - **Mitigation:** Comprehensive dependency mapping, staged rollout
   - **Contingency:** Maintain old models during transition

### Medium Risk Items
1. **Vocab Layer Refactoring**
   - **Risk:** Concept mapping errors
   - **Mitigation:** Extensive validation against OMOP vocabulary
   - **Contingency:** Automated concept accuracy tests

2. **SQL Query Optimization**
   - **Risk:** Logic errors in refactored queries
   - **Mitigation:** Row count validation, parallel execution
   - **Contingency:** Git rollback to previous version

### Low Risk Items
1. **Documentation Updates**
2. **Performance Monitoring**
3. **DISTINCT Clause Removal**

---

## 8. Success Metrics

### Performance Metrics
- **Overall Pipeline Runtime:** Target 50% reduction
- **Incremental Model Efficiency:** >80% time savings vs FULL
- **Query Execution Time:** 30-50% improvement for optimized queries
- **Resource Utilization:** Reduced CPU and memory usage

### Quality Metrics
- **Data Accuracy:** 100% row count match vs baseline
- **Concept Mapping Accuracy:** No regression in OMOP concept IDs
- **Foreign Key Integrity:** 0 orphaned records
- **Test Coverage:** >90% of OMOP tables with automated tests

### Operational Metrics
- **Model Count Reduction:** 10-15 fewer models
- **Code Maintainability:** Reduced complexity in vocab layer
- **Pipeline Observability:** Performance dashboards in place
- **Documentation Completeness:** All changes documented

---

## 9. Timeline Summary

| Phase | Duration | Effort (hours) | Expected Impact |
|-------|----------|----------------|-----------------|
| Phase 1: Quick Wins | 2 weeks | 20-30 | 15-25% faster |
| Phase 2: Consolidation | 2 weeks | 30-40 | 10 fewer models |
| Phase 3: Incremental | 4 weeks | 60-80 | 60-80% faster facts |
| Phase 4: Vocab Optimization | 2 weeks | 25-35 | 30-50% faster vocab |
| Phase 5: Advanced | 2 weeks | 30-40 | 10-20% additional |
| **Total** | **12 weeks** | **165-225 hours** | **Overall 70%+ improvement** |

---

## 10. Next Steps

### Immediate Actions
1. **Review this plan** with data engineering team
2. **Prioritize subtasks** based on business value and risk
3. **Assign ownership** for each subtask
4. **Set up project tracking** (Jira/GitHub Issues)
5. **Create feature branches** for each phase

### Ongoing Activities
- Weekly progress reviews
- Performance monitoring dashboard
- Continuous testing and validation
- Documentation updates
- Knowledge sharing sessions

---

## 11. Appendix

### A. Model Inventory

See detailed model analysis in sections 1-3.

### B. Dependency Graph

Generate with:
```bash
sqlmesh dag --select-model +cdm.{table_name}+
```

Key tables to visualize:
- person
- visit_occurrence
- measurement
- observation
- condition_occurrence

### C. References

- SQLMesh Documentation: https://sqlmesh.readthedocs.io/
- OMOP CDM Specification: https://ohdsi.github.io/CommonDataModel/
- SQL Server Best Practices: https://docs.microsoft.com/sql/
- Project Copilot Instructions: `.github/copilot-instructions.md`

### D. Contact

For questions about this optimization plan:
- **Data Engineering Team:** [contact]
- **Project Owner:** LTH DST
- **SQLMesh Support:** [support]

---

**Document Control:**
- **Created:** 2026-02-14
- **Last Updated:** 2026-02-14
- **Version:** 1.0
- **Status:** Ready for Review
- **Next Review:** After Phase 1 completion
