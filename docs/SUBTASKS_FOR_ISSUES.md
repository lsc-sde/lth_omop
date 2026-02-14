# LTH OMOP ETL - Subtasks for GitHub Issues

This document provides ready-to-use issue descriptions for implementing the optimization plan. Each subtask can be converted into a GitHub issue and assigned to team members.

---

## 📋 Quick Reference

| Category | Subtasks | Total Effort | Priority |
|----------|----------|--------------|----------|
| SQL Optimization | 4 | 7-11 hours | HIGH |
| Model Consolidation | 5 | 16-23 hours | MEDIUM |
| Incremental Loading | 5 | 30-42 hours | HIGH |
| Vocab Optimization | 2 | 10-14 hours | MEDIUM |
| Testing & Validation | 2 | 12-16 hours | HIGH |
| **TOTAL** | **18** | **75-106 hours** | - |

---

## Category 1: SQL Query Optimization

### Issue 1.1: Optimize vocab__observation Query Performance

**Labels:** `optimization`, `sql`, `vocab-layer`, `priority:high`  
**Effort:** 2-3 hours  
**Assignee:** Backend Developer

**Description:**

The `vocab__observation` model is the most complex vocab model with 196 lines, 9 JOINs, and 3 CTEs. This task optimizes the query structure to improve execution time.

**Current Issues:**
- Multiple repeated JOINs to `vocab__source_to_concept_map` with different filters
- Inline subqueries that could be CTEs
- Potential for pushing filters earlier in the pipeline

**Tasks:**
- [ ] Create dedicated CTEs for each `concept_group` filter
- [ ] Replace repeated `source_to_concept_map` subqueries with CTEs
- [ ] Analyze if DISTINCT is necessary and remove if not
- [ ] Test query performance before/after with production data volumes
- [ ] Validate result accuracy (row count, concept mappings)

**Files to Modify:**
- `models/vocab/vocab__observation.sql`

**Acceptance Criteria:**
- Query executes 30%+ faster than baseline
- Row count matches original model output
- All concept mappings preserved and accurate
- No regression in data quality

**Performance Baseline:**
```bash
# Run before changes
sqlmesh evaluate lth_bronze.vocab__observation --timing
```

**Reference:**
See Section 4.2 of OPTIMIZATION_PLAN.md

---

### Issue 1.2: Optimize vocab__condition_occurrence Query

**Labels:** `optimization`, `sql`, `vocab-layer`, `priority:high`  
**Effort:** 2-3 hours  
**Assignee:** Backend Developer

**Description:**

The `vocab__condition_occurrence` model has 97 lines and 8 JOINs, including complex ICD10→SNOMED mapping logic that can be optimized.

**Current Issues:**
- Multiple JOINs to OMOP vocabulary tables
- ICD10 to SNOMED concept_relationship mappings repeated
- Potential for materializing mappings

**Tasks:**
- [ ] Materialize ICD10→SNOMED mappings in a dedicated CTE
- [ ] Reduce redundant JOINs to vocabulary tables
- [ ] Push concept filtering earlier in the query
- [ ] Test query performance with production volumes
- [ ] Validate concept mapping accuracy

**Files to Modify:**
- `models/vocab/vocab__condition_occurrence.sql`

**Acceptance Criteria:**
- Query executes 40%+ faster
- ICD10 to SNOMED mappings are 100% correct
- No missing concept IDs
- All condition records preserved

**Testing:**
```sql
-- Validate concept mappings
SELECT COUNT(*), COUNT(DISTINCT condition_concept_id)
FROM lth_bronze.vocab__condition_occurrence
```

**Reference:**
See Section 4.2 of OPTIMIZATION_PLAN.md

---

### Issue 1.3: Optimize stg_sl__bacteriology Query

**Labels:** `optimization`, `sql`, `staging-layer`, `priority:medium`  
**Effort:** 1-2 hours  
**Assignee:** Backend Developer

**Description:**

The `stg_sl__bacteriology` model (118 lines) performs UNION ALL of live and archive bacteriology data with the same filtering applied twice.

**Current Issues:**
- Invalid results filter applied separately to each UNION branch
- Same filtering logic duplicated
- Opportunity to filter before UNION

**Tasks:**
- [ ] Apply `invalid_results` filter before UNION if possible
- [ ] Move common filters to a CTE
- [ ] Test with production data volumes
- [ ] Verify deduplication logic still works

**Files to Modify:**
- `models/staging/swisslab/stg_sl__bacteriology.sql`

**Acceptance Criteria:**
- Query executes 20%+ faster
- Row counts match original (validate against baseline)
- No duplicate records in output
- All valid bacteriology results included

**Validation:**
```sql
-- Check for duplicates
SELECT order_number, isolate_number, COUNT(*)
FROM lth_bronze.stg_sl__bacteriology
GROUP BY order_number, isolate_number
HAVING COUNT(*) > 1
```

**Reference:**
See Section 4.2 of OPTIMIZATION_PLAN.md

---

### Issue 1.4: Remove Redundant DISTINCT Clauses

**Labels:** `optimization`, `sql`, `code-quality`, `priority:medium`  
**Effort:** 2-3 hours  
**Assignee:** Backend Developer

**Description:**

Audit all models for unnecessary `DISTINCT` usage. `DISTINCT` is expensive and often unnecessary when proper keys are used.

**Tasks:**
- [ ] Scan all SQL models for DISTINCT usage
- [ ] Analyze each DISTINCT to determine if necessary
  - Check if query naturally produces unique rows
  - Check if there are natural keys that guarantee uniqueness
- [ ] Remove DISTINCT where unnecessary
- [ ] Add comments explaining why DISTINCT is needed where kept
- [ ] Run tests to verify row counts unchanged

**Files to Analyze:**
- All `models/vocab/*.sql`
- All `models/staging/**/*.sql`
- All `models/omop/*.sql`

**Acceptance Criteria:**
- Document created listing all DISTINCT usage and decisions
- Remove 30%+ of unnecessary DISTINCT clauses
- No change in output row counts for any model
- Comments added where DISTINCT is kept

**Review Script:**
```bash
# Find all DISTINCT usage
grep -r "SELECT DISTINCT" models/ --include="*.sql"
```

**Reference:**
See Section 4.2 of OPTIMIZATION_PLAN.md

---

## Category 2: Model Consolidation

### Issue 2.1: Consolidate Flex Care Site Source Models

**Labels:** `refactoring`, `source-layer`, `consolidation`, `priority:medium`  
**Effort:** 4-6 hours  
**Assignee:** Data Engineer

**Description:**

Currently there are 3 separate care site source models for GP, IP, and OP sites. These can be consolidated into a single model with a type discriminator.

**Current Models:**
- `src_flex__care_site_gp` - GP care sites
- `src_flex__care_site_ip` - Inpatient care sites
- `src_flex__care_site_op` - Outpatient care sites

**Tasks:**
- [ ] Create new `models/source/flex/src_flex__care_site.sql`
- [ ] UNION ALL the 3 models with added `care_site_type` column
- [ ] Ensure all columns from all 3 models are included
- [ ] Update `stg_flex__care_site` to reference new consolidated model
- [ ] Test downstream dependencies (vocab__care_site, omop/care_site)
- [ ] Validate row counts: new model = sum of 3 old models
- [ ] Remove old 3 models once validated

**New Model Structure:**
```sql
-- Consolidated model with type discriminator
SELECT *, 'GP' AS care_site_type FROM ...gp source
UNION ALL
SELECT *, 'IP' AS care_site_type FROM ...ip source
UNION ALL
SELECT *, 'OP' AS care_site_type FROM ...op source
```

**Files to Create:**
- `models/source/flex/src_flex__care_site.sql` (new)

**Files to Modify:**
- `models/staging/flex/stg_flex__care_site.sql`

**Files to Delete (after validation):**
- `models/source/flex/src_flex__care_site_gp.sql`
- `models/source/flex/src_flex__care_site_ip.sql`
- `models/source/flex/src_flex__care_site_op.sql`

**Acceptance Criteria:**
- Single source model replaces 3 models
- All care site types (GP, IP, OP) preserved
- Downstream models work correctly
- Row count in new model = sum of 3 old models
- All tests pass

**Testing:**
```sql
-- Validate row counts
SELECT care_site_type, COUNT(*) 
FROM lth_bronze.src_flex__care_site 
GROUP BY care_site_type;
```

**Reference:**
See Section 4.1 Priority 1 of OPTIMIZATION_PLAN.md

---

### Issue 2.2: Consolidate Flex Employee Source Models

**Labels:** `refactoring`, `source-layer`, `consolidation`, `priority:medium`  
**Effort:** 4-6 hours  
**Assignee:** Data Engineer

**Description:**

There are 3 employee-related source models that may be candidates for consolidation or better organization.

**Current Models:**
- `src_flex__emp_type` - Employee types
- `src_flex__emp_provider` - Employee as provider
- `src_flex__emp_consultant` - Consultant employees

**Tasks:**
- [ ] Analyze the 3 models to understand their purpose
- [ ] Determine if consolidation is appropriate or if they should remain separate
- [ ] If consolidating:
  - [ ] Create `src_flex__employee.sql` with UNION ALL
  - [ ] Add `employee_type` discriminator column
  - [ ] Update staging provider references
- [ ] If keeping separate:
  - [ ] Document why they should remain separate
  - [ ] Consider adding clear comments to each model
- [ ] Test provider assignment logic
- [ ] Validate no missing provider data
- [ ] Remove old models if consolidated

**Files to Analyze:**
- `models/source/flex/src_flex__emp_type.sql`
- `models/source/flex/src_flex__emp_provider.sql`
- `models/source/flex/src_flex__emp_consultant.sql`

**Files to Modify:**
- `models/staging/flex/stg_flex__provider.sql` (if consolidated)

**Acceptance Criteria:**
- Provider data integrity maintained
- No missing provider assignments in visit_occurrence
- All provider types preserved
- Clear documentation of decision

**Reference:**
See Section 4.1 Priority 1 of OPTIMIZATION_PLAN.md

---

### Issue 2.3: Consolidate Flex Diagnosis Source Models

**Labels:** `refactoring`, `source-layer`, `consolidation`, `priority:medium`  
**Effort:** 3-4 hours  
**Assignee:** Data Engineer

**Description:**

There are 2 diagnosis source models (AE and VTG) that can be consolidated into a single model with source tracking.

**Current Models:**
- `src_flex__ae_diagnosis` - A&E diagnoses
- `src_flex__vtg_diagnosis` - VTG diagnoses

**Tasks:**
- [ ] Create new `models/source/flex/src_flex__diagnosis.sql`
- [ ] UNION ALL the 2 models with added `diagnosis_source` column ('AE', 'VTG')
- [ ] Ensure all diagnosis codes and fields preserved
- [ ] Update `stg_flex__condition_occurrence` to use new model
- [ ] Test condition occurrence mappings
- [ ] Validate row counts match sum of originals
- [ ] Remove old 2 models after validation

**Files to Create:**
- `models/source/flex/src_flex__diagnosis.sql`

**Files to Modify:**
- `models/staging/flex/stg_flex__condition_occurrence.sql`

**Files to Delete (after validation):**
- `models/source/flex/src_flex__ae_diagnosis.sql`
- `models/source/flex/src_flex__vtg_diagnosis.sql`

**Acceptance Criteria:**
- All diagnosis codes preserved
- Source tracking maintained (AE vs VTG)
- Condition occurrence mappings accurate
- Row count = sum of 2 old models

**Reference:**
See Section 4.1 Priority 1 of OPTIMIZATION_PLAN.md

---

### Issue 2.4: Consolidate Flex Procedure Source Models

**Labels:** `refactoring`, `source-layer`, `consolidation`, `priority:medium`  
**Effort:** 3-4 hours  
**Assignee:** Data Engineer

**Description:**

There are 2 procedure source models (AE and VTG) that can be consolidated.

**Current Models:**
- `src_flex__ae_procedures` - A&E procedures
- `src_flex__vtg_procedure` - VTG procedures

**Tasks:**
- [ ] Create new `models/source/flex/src_flex__procedure.sql`
- [ ] UNION ALL with `procedure_source` discriminator ('AE', 'VTG')
- [ ] Preserve all procedure codes
- [ ] Update `stg_flex__procedure_occurrence`
- [ ] Test procedure mappings
- [ ] Validate row counts
- [ ] Remove old models

**Files to Create:**
- `models/source/flex/src_flex__procedure.sql`

**Files to Modify:**
- `models/staging/flex/stg_flex__procedure_occurrence.sql`

**Files to Delete (after validation):**
- `models/source/flex/src_flex__ae_procedures.sql`
- `models/source/flex/src_flex__vtg_procedure.sql`

**Acceptance Criteria:**
- All procedure codes preserved
- Source attribution maintained
- Procedure occurrence mappings correct

**Reference:**
See Section 4.1 Priority 1 of OPTIMIZATION_PLAN.md

---

### Issue 2.5: Consolidate Flex Device Source Models

**Labels:** `refactoring`, `source-layer`, `consolidation`, `priority:low`  
**Effort:** 2-3 hours  
**Assignee:** Data Engineer

**Description:**

There are 2 device source models that can be consolidated.

**Current Models:**
- `src_flex__implant_devices` - Implanted devices
- `src_flex__cathether_devices` - Catheter devices

**Tasks:**
- [ ] Create new `models/source/flex/src_flex__device.sql`
- [ ] UNION ALL with `device_type` discriminator ('implant', 'catheter')
- [ ] Ensure all device records included
- [ ] Update `stg_flex__devices`
- [ ] Test device exposure mappings
- [ ] Remove old models

**Files to Create:**
- `models/source/flex/src_flex__device.sql`

**Files to Modify:**
- `models/staging/flex/stg_flex__devices.sql`

**Files to Delete (after validation):**
- `models/source/flex/src_flex__implant_devices.sql`
- `models/source/flex/src_flex__cathether_devices.sql`

**Acceptance Criteria:**
- All devices tracked
- Device type classification correct
- Device exposure records accurate

**Reference:**
See Section 4.1 Priority 1 of OPTIMIZATION_PLAN.md

---

## Category 3: Incremental Loading Implementation

### Issue 3.1: Convert observation to INCREMENTAL_BY_TIME_RANGE

**Labels:** `enhancement`, `performance`, `incremental-loading`, `priority:high`  
**Effort:** 8-12 hours  
**Assignee:** Senior Data Engineer

**Description:**

Convert the `observation` OMOP table from FULL refresh to incremental loading using INCREMENTAL_BY_TIME_RANGE strategy. This is a large fact table that will benefit significantly from incremental processing.

**Current State:**
- Model kind: FULL
- Daily complete rebuild
- Large data volume

**Target State:**
- Model kind: INCREMENTAL_BY_TIME_RANGE
- Batch size: 30 days
- Batch concurrency: 4
- Time column: `updated_at`

**Tasks:**
- [ ] Verify `updated_at` column exists and is reliable in vocab__observation
- [ ] Change model kind in `models/omop/observation.sql`
- [ ] Add time filter: `WHERE updated_at BETWEEN @start_ds AND @end_ds`
- [ ] Configure batch_size (start with 30 days)
- [ ] Configure batch_concurrency (start with 4)
- [ ] Test incremental load with sample date range (1 week)
- [ ] Validate results match full refresh for test period
- [ ] Test idempotency (re-run same date range)
- [ ] Backfill historical data in batches
- [ ] Monitor incremental efficiency
- [ ] Document configuration choices

**Model Configuration:**
```sql
MODEL (
  name lth_bronze.observation,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column updated_at,
    batch_size 30,
    batch_concurrency 4
  ),
  cron '@daily'
)
```

**Files to Modify:**
- `models/omop/observation.sql`
- Potentially `models/vocab/vocab__observation.sql` (if updated_at missing)

**Acceptance Criteria:**
- Incremental loads produce identical results to full refresh
- No duplicate records (test with overlapping date ranges)
- Performance improvement >60% vs full refresh
- Idempotent (can safely re-run same date range)
- Historical data backfilled successfully
- Documentation updated

**Testing Plan:**
```bash
# 1. Baseline full refresh
sqlmesh plan --start-date 2024-01-01 --end-date 2024-01-07

# 2. Test incremental load
sqlmesh plan --start-date 2024-01-08 --end-date 2024-01-14

# 3. Test overlapping range (idempotency)
sqlmesh plan --start-date 2024-01-08 --end-date 2024-01-14

# 4. Compare row counts
sqlmesh fetchdf "SELECT COUNT(*) FROM lth_bronze.observation"
```

**Rollback Plan:**
If issues arise, revert model kind to FULL and redeploy.

**Reference:**
- See Section 4.1 Priority 4 of OPTIMIZATION_PLAN.md
- Reference implementation: `models/omop/measurement.sql` (already incremental)

---

### Issue 3.2: Convert condition_occurrence to INCREMENTAL_BY_TIME_RANGE

**Labels:** `enhancement`, `performance`, `incremental-loading`, `priority:high`  
**Effort:** 6-8 hours  
**Assignee:** Senior Data Engineer

**Description:**

Convert `condition_occurrence` to incremental loading. This is a high-volume fact table.

**Tasks:**
- [ ] Verify updated_at tracking in vocab__condition_occurrence
- [ ] Convert model to INCREMENTAL_BY_TIME_RANGE
- [ ] Add time filter predicate
- [ ] Configure batch_size and batch_concurrency
- [ ] Test incremental behavior
- [ ] Backfill historical data
- [ ] Performance benchmark

**Files to Modify:**
- `models/omop/condition_occurrence.sql`

**Acceptance Criteria:**
- Incremental loads correct (validate against full refresh)
- Performance improvement >60%
- No missing conditions

**Reference:**
See Section 4.1 Priority 4 of OPTIMIZATION_PLAN.md

---

### Issue 3.3: Convert procedure_occurrence to INCREMENTAL_BY_TIME_RANGE

**Labels:** `enhancement`, `performance`, `incremental-loading`, `priority:high`  
**Effort:** 6-8 hours  
**Assignee:** Senior Data Engineer

**Description:**

Convert `procedure_occurrence` to incremental loading for improved performance.

**Tasks:**
- [ ] Verify updated_at tracking
- [ ] Convert to INCREMENTAL_BY_TIME_RANGE
- [ ] Add time filter
- [ ] Test and backfill
- [ ] Benchmark performance

**Files to Modify:**
- `models/omop/procedure_occurrence.sql`

**Acceptance Criteria:**
- Incremental loads accurate
- Performance improvement >60%
- All procedures captured

**Reference:**
See Section 4.1 Priority 4 of OPTIMIZATION_PLAN.md

---

### Issue 3.4: Convert drug_exposure to INCREMENTAL_BY_TIME_RANGE

**Labels:** `enhancement`, `performance`, `incremental-loading`, `priority:high`  
**Effort:** 6-8 hours  
**Assignee:** Senior Data Engineer

**Description:**

Convert `drug_exposure` to incremental loading. Large volume table.

**Tasks:**
- [ ] Verify updated_at column
- [ ] Convert to INCREMENTAL_BY_TIME_RANGE
- [ ] Add time filter
- [ ] Test and backfill
- [ ] Benchmark

**Files to Modify:**
- `models/omop/drug_exposure.sql`

**Acceptance Criteria:**
- Incremental loads correct
- Performance improvement >70%
- Drug records complete

**Reference:**
See Section 4.1 Priority 4 of OPTIMIZATION_PLAN.md

---

### Issue 3.5: Convert device_exposure to INCREMENTAL_BY_TIME_RANGE

**Labels:** `enhancement`, `performance`, `incremental-loading`, `priority:medium`  
**Effort:** 4-6 hours  
**Assignee:** Senior Data Engineer

**Description:**

Convert `device_exposure` to incremental loading.

**Tasks:**
- [ ] Verify updated_at tracking
- [ ] Convert to INCREMENTAL_BY_TIME_RANGE
- [ ] Add time filter
- [ ] Test and backfill
- [ ] Benchmark

**Files to Modify:**
- `models/omop/device_exposure.sql`

**Acceptance Criteria:**
- Incremental loads accurate
- Performance improvement >50%
- Device exposure complete

**Reference:**
See Section 4.1 Priority 4 of OPTIMIZATION_PLAN.md

---

## Category 4: Vocab Layer Optimization

### Issue 4.1: Create Domain-Specific Concept Map Views

**Labels:** `enhancement`, `vocab-layer`, `performance`, `priority:medium`  
**Effort:** 4-6 hours  
**Assignee:** Data Engineer

**Description:**

Create pre-filtered concept mapping views for each OMOP domain to simplify and speed up vocab model queries.

**Problem:**
Currently, every vocab model filters `vocab__source_to_concept_map` by `concept_group` inline, causing:
- Repeated filtering logic
- Complex subqueries
- Slower query execution

**Solution:**
Create dedicated views for each domain that pre-filter concept groups.

**Tasks:**
- [ ] Create `models/vocab/vocab__concept_map_measurement.sql`
  - Filter: `concept_group IN ('result', 'units', 'decoded', 'bacteria_presence', 'bacteria_sensitivities', 'bacteriology_other_test', 'bacteriology_other_result')`
- [ ] Create `models/vocab/vocab__concept_map_condition.sql`
  - Filter: `concept_group IN ('diagnosis', 'condition')`
- [ ] Create `models/vocab/vocab__concept_map_procedure.sql`
  - Filter: `concept_group IN ('procedure')`
- [ ] Create `models/vocab/vocab__concept_map_drug.sql`
  - Filter: `concept_group IN ('drug', 'drug_routes')`
- [ ] Create `models/vocab/vocab__concept_map_device.sql`
  - Filter: `concept_group IN ('device')`
- [ ] All models should be kind FULL (refresh daily)
- [ ] Test that all concept_groups are covered
- [ ] Validate row counts match filtered queries

**Model Template:**
```sql
MODEL (
  name lth_bronze.vocab__concept_map_measurement,
  kind FULL,
  cron '@daily'
);

SELECT *
FROM lth_bronze.vocab__source_to_concept_map
WHERE concept_group IN (
  'result',
  'units',
  'decoded',
  'bacteria_presence',
  'bacteria_sensitivities',
  'bacteriology_other_test',
  'bacteriology_other_result'
)
```

**Files to Create:**
- `models/vocab/vocab__concept_map_measurement.sql`
- `models/vocab/vocab__concept_map_condition.sql`
- `models/vocab/vocab__concept_map_procedure.sql`
- `models/vocab/vocab__concept_map_drug.sql`
- `models/vocab/vocab__concept_map_device.sql`

**Acceptance Criteria:**
- 5 new concept map views created
- No missing concept_groups
- Views materialize correctly
- Row counts validated

**Reference:**
See Section 4.1 Priority 3 of OPTIMIZATION_PLAN.md

---

### Issue 4.2: Refactor Vocab Models to Use Pre-filtered Concept Maps

**Labels:** `refactoring`, `vocab-layer`, `performance`, `priority:medium`  
**Effort:** 6-8 hours  
**Assignee:** Data Engineer  
**Depends On:** Issue 4.1

**Description:**

Refactor all vocab models to use the new pre-filtered concept map views created in Issue 4.1.

**Tasks:**
- [ ] Update `vocab__measurement.sql` to use `vocab__concept_map_measurement`
  - Replace inline filtered subqueries
  - Simplify JOINs
- [ ] Update `vocab__condition_occurrence.sql` to use `vocab__concept_map_condition`
- [ ] Update `vocab__procedure_occurrence.sql` to use `vocab__concept_map_procedure`
- [ ] Update `vocab__drug_exposure.sql` to use `vocab__concept_map_drug`
- [ ] Update `vocab__device_exposure.sql` to use `vocab__concept_map_device`
- [ ] Remove inline `concept_group` filters from all models
- [ ] Test all vocab models for accuracy (row counts, concept mappings)
- [ ] Performance benchmark each model (before/after)
- [ ] Update documentation

**Example Refactoring:**

**Before:**
```sql
LEFT JOIN (
  SELECT source_code, target_concept_id
  FROM lth_bronze.vocab__source_to_concept_map
  WHERE concept_group = 'result'
) AS cm ON r.source_code = cm.source_code
```

**After:**
```sql
LEFT JOIN lth_bronze.vocab__concept_map_measurement AS cm
  ON r.source_code = cm.source_code
```

**Files to Modify:**
- `models/vocab/vocab__measurement.sql`
- `models/vocab/vocab__condition_occurrence.sql`
- `models/vocab/vocab__procedure_occurrence.sql`
- `models/vocab/vocab__drug_exposure.sql`
- `models/vocab/vocab__device_exposure.sql`

**Acceptance Criteria:**
- All vocab models refactored
- Concept mappings identical to before (100% match)
- Performance improvement 30-50% per model
- Cleaner, more readable SQL
- All tests passing

**Validation:**
```sql
-- Compare concept_id distributions before/after
SELECT concept_id, COUNT(*)
FROM lth_bronze.vocab__measurement
GROUP BY concept_id
ORDER BY concept_id;
```

**Reference:**
See Section 4.1 Priority 3 of OPTIMIZATION_PLAN.md

---

## Category 5: Testing and Validation

### Issue 5.1: Create Comprehensive Data Quality Test Suite

**Labels:** `testing`, `quality`, `automation`, `priority:high`  
**Effort:** 8-10 hours  
**Assignee:** QA Engineer

**Description:**

Create an automated test suite to validate data quality across all OMOP tables and ensure optimizations don't introduce regressions.

**Test Categories:**

1. **Row Count Validation**
   - Baseline row counts for all OMOP tables
   - Alert on unexpected changes (>10% difference)

2. **Concept Mapping Accuracy**
   - Validate concept_id values are valid OMOP concepts
   - Check for unmapped codes (concept_id = 0)
   - Verify domain assignments

3. **Foreign Key Integrity**
   - person_id exists in person table
   - visit_occurrence_id exists in visit_occurrence
   - provider_id exists in provider
   - care_site_id exists in care_site

4. **Incremental Idempotency**
   - Test re-running same date range produces same results
   - No duplicate records

5. **Data Lineage**
   - Source system attribution preserved
   - org_code tracking correct

**Tasks:**
- [ ] Create `tests/test_row_counts.sql`
- [ ] Create `tests/test_concept_mappings.sql`
- [ ] Create `tests/test_foreign_keys.sql`
- [ ] Create `tests/test_incremental_idempotency.sql`
- [ ] Create `tests/test_data_lineage.sql`
- [ ] Set up automated test execution in CI/CD
- [ ] Create test documentation
- [ ] Baseline current metrics for comparison

**Test Framework:**

Use SQLMesh's built-in testing:
```sql
-- tests/test_person_row_count.sql
AUDIT (
  name person_row_count_check,
  dialect tsql
);

SELECT COUNT(*) AS row_count
FROM lth_bronze.person
HAVING COUNT(*) < 1000  -- Alert if less than baseline
```

**Files to Create:**
- `tests/test_row_counts.sql`
- `tests/test_concept_mappings.sql`
- `tests/test_foreign_keys.sql`
- `tests/test_incremental_idempotency.sql`
- `tests/test_data_lineage.sql`
- `docs/TESTING.md` (test documentation)

**Acceptance Criteria:**
- Comprehensive test coverage (>90% of OMOP tables)
- All baseline tests passing
- Automated execution in CI/CD pipeline
- Clear documentation of test expectations
- Alerting mechanism for failures

**Reference:**
See Section 6.5 of OPTIMIZATION_PLAN.md

---

### Issue 5.2: Create Performance Benchmarking Framework

**Labels:** `monitoring`, `performance`, `documentation`, `priority:medium`  
**Effort:** 4-6 hours  
**Assignee:** Data Engineer

**Description:**

Create a framework to measure and track performance improvements from optimization efforts.

**Metrics to Track:**
1. Model execution time (per model)
2. Row counts (per model)
3. Resource utilization (CPU, memory)
4. Pipeline total runtime
5. Incremental efficiency (rows processed vs total)

**Tasks:**
- [ ] Create baseline performance measurements
- [ ] Set up performance tracking queries
- [ ] Create performance comparison reports
- [ ] Document execution times for all models
- [ ] Track resource utilization metrics
- [ ] Create performance dashboard (optional)
- [ ] Generate optimization ROI analysis

**Deliverables:**
1. **Baseline Report** (`docs/PERFORMANCE_BASELINE.md`)
   - Current execution times
   - Current row counts
   - Current resource usage

2. **Benchmark Queries** (`scripts/benchmark.sql`)
   - Timing queries for each model
   - Row count validation
   - Resource monitoring

3. **Performance Dashboard** (optional)
   - Grafana/PowerBI dashboard
   - Real-time performance metrics
   - Trend analysis

4. **ROI Analysis** (`docs/OPTIMIZATION_ROI.md`)
   - Time savings per optimization
   - Cost savings (compute resources)
   - Maintenance effort reduction

**Files to Create:**
- `docs/PERFORMANCE_BASELINE.md`
- `docs/OPTIMIZATION_ROI.md`
- `scripts/benchmark.sql`

**Acceptance Criteria:**
- Baseline metrics documented
- Benchmarking queries operational
- Performance tracking automated
- Clear ROI analysis for each optimization

**Example Metrics:**

| Model | Baseline Time | Optimized Time | Improvement | Row Count |
|-------|---------------|----------------|-------------|-----------|
| observation | 45 min | 18 min | 60% | 5.2M |
| measurement | 30 min | 12 min | 60% | 8.1M |
| condition_occurrence | 25 min | 10 min | 60% | 2.3M |

**Reference:**
See Section 8 of OPTIMIZATION_PLAN.md

---

## 🎯 Suggested Implementation Order

### Sprint 1 (Week 1-2): Quick Wins
1. Issue 1.4: Remove Redundant DISTINCT Clauses
2. Issue 1.3: Optimize stg_sl__bacteriology Query
3. Issue 1.1: Optimize vocab__observation Query
4. Issue 1.2: Optimize vocab__condition_occurrence Query

**Expected Impact:** 15-25% overall improvement

### Sprint 2 (Week 3-4): Model Consolidation
1. Issue 2.1: Consolidate Flex Care Site Models
2. Issue 2.3: Consolidate Flex Diagnosis Models
3. Issue 2.4: Consolidate Flex Procedure Models
4. Issue 2.5: Consolidate Flex Device Models
5. Issue 2.2: Consolidate Flex Employee Models (if decided)

**Expected Impact:** 10 fewer models

### Sprint 3 (Week 5-6): Testing Foundation
1. Issue 5.1: Create Data Quality Test Suite
2. Issue 5.2: Create Performance Benchmarking Framework

**Expected Impact:** Quality assurance foundation

### Sprint 4-5 (Week 7-10): Incremental Loading
1. Issue 3.1: Convert observation to INCREMENTAL
2. Issue 3.2: Convert condition_occurrence to INCREMENTAL
3. Issue 3.3: Convert procedure_occurrence to INCREMENTAL
4. Issue 3.4: Convert drug_exposure to INCREMENTAL
5. Issue 3.5: Convert device_exposure to INCREMENTAL

**Expected Impact:** 60-80% faster fact table refresh

### Sprint 6 (Week 11-12): Vocab Optimization
1. Issue 4.1: Create Domain Concept Map Views
2. Issue 4.2: Refactor Vocab Models

**Expected Impact:** 30-50% faster vocab layer

---

## 📊 Effort Summary

| Sprint | Issues | Effort Range | Priority |
|--------|--------|--------------|----------|
| Sprint 1 | 4 | 7-11 hours | HIGH |
| Sprint 2 | 5 | 16-23 hours | MEDIUM |
| Sprint 3 | 2 | 12-16 hours | HIGH |
| Sprint 4-5 | 5 | 30-42 hours | HIGH |
| Sprint 6 | 2 | 10-14 hours | MEDIUM |
| **Total** | **18** | **75-106 hours** | - |

---

## 📝 Notes for Issue Creation

When creating GitHub issues from these subtasks:

1. **Copy the entire subtask content** into the issue description
2. **Add appropriate labels** as specified
3. **Set milestone** based on sprint assignment
4. **Link to optimization plan**: Reference `docs/OPTIMIZATION_PLAN.md`
5. **Add acceptance criteria** as issue checklist
6. **Assign to team member** with appropriate expertise

Example issue command:
```bash
gh issue create \
  --title "Optimize vocab__observation Query Performance" \
  --body-file subtasks/issue-1-1.md \
  --label "optimization,sql,vocab-layer,priority:high" \
  --milestone "Sprint 1" \
  --assignee backend-dev
```

---

**Document Control:**
- **Created:** 2026-02-14
- **Last Updated:** 2026-02-14
- **Version:** 1.0
- **Related:** `docs/OPTIMIZATION_PLAN.md`
