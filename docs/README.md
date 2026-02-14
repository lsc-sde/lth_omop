# LTH OMOP ETL Optimization Documentation

This folder contains comprehensive documentation for optimizing the LTH OMOP ETL SQLMesh project.

## 📚 Documentation Files

### [OPTIMIZATION_PLAN.md](./OPTIMIZATION_PLAN.md)
**Comprehensive optimization plan for the entire SQLMesh project**

This is the main document containing:
- Complete architecture analysis
- Model dependency mapping
- Performance analysis and bottlenecks
- Detailed optimization recommendations
- SQL query optimization guidelines
- Implementation phases and timeline
- Success metrics and KPIs

**Use this for:** Understanding the overall optimization strategy, technical decisions, and roadmap.

### [SUBTASKS_FOR_ISSUES.md](./SUBTASKS_FOR_ISSUES.md)
**Ready-to-use GitHub issue descriptions**

Contains 18 detailed subtasks that can be directly converted into GitHub issues:
- **Category 1:** SQL Query Optimization (4 issues)
- **Category 2:** Model Consolidation (5 issues)
- **Category 3:** Incremental Loading (5 issues)
- **Category 4:** Vocab Optimization (2 issues)
- **Category 5:** Testing & Validation (2 issues)

Each subtask includes:
- Detailed task description
- Effort estimation (hours)
- Files to modify
- Acceptance criteria
- Testing approach
- Code examples

**Use this for:** Creating GitHub issues and assigning work to team members.

## 🎯 Quick Reference

### Optimization Impact Summary

| Optimization Area | Expected Improvement | Effort | Priority |
|-------------------|---------------------|--------|----------|
| SQL Query Optimization | 15-25% faster | LOW | HIGH |
| Model Consolidation | 10 fewer models | MEDIUM | MEDIUM |
| Incremental Loading | 60-80% faster facts | HIGH | HIGH |
| Vocab Optimization | 30-50% faster vocab | MEDIUM | MEDIUM |
| **Total Impact** | **70%+ overall** | **75-106 hours** | - |

### Implementation Timeline

| Phase | Duration | Key Activities |
|-------|----------|----------------|
| Phase 1: Quick Wins | 2 weeks | SQL optimizations, remove redundant DISTINCT |
| Phase 2: Consolidation | 2 weeks | Merge source models (3→1, 2→1) |
| Phase 3: Incremental | 4 weeks | Convert 5 OMOP tables to incremental |
| Phase 4: Vocab Optimization | 2 weeks | Create concept map views, refactor |
| Phase 5: Advanced | 2 weeks | Monitoring, indexing, documentation |
| **Total** | **12 weeks** | **Full optimization** |

## 🚀 Getting Started

### For Project Managers

1. Read the Executive Summary in [OPTIMIZATION_PLAN.md](./OPTIMIZATION_PLAN.md#executive-summary)
2. Review the [Implementation Plan](./OPTIMIZATION_PLAN.md#5-implementation-plan)
3. Use [SUBTASKS_FOR_ISSUES.md](./SUBTASKS_FOR_ISSUES.md) to create GitHub issues
4. Assign issues to team members based on expertise

### For Data Engineers

1. Review the [Architecture Analysis](./OPTIMIZATION_PLAN.md#1-architecture-analysis)
2. Study the [Optimization Recommendations](./OPTIMIZATION_PLAN.md#4-optimization-recommendations)
3. Pick up issues from [SUBTASKS_FOR_ISSUES.md](./SUBTASKS_FOR_ISSUES.md)
4. Follow the acceptance criteria and testing approach for each issue

### For QA Engineers

1. Review [Testing and Validation](./OPTIMIZATION_PLAN.md#testing--validation-subtasks) section
2. Implement data quality tests from Issue 5.1
3. Set up performance benchmarking from Issue 5.2
4. Validate all changes against acceptance criteria

## 📊 Key Metrics

### Current State
- **Total Models:** 126 (SQL + seeds)
- **Architecture:** 4 layers (source → staging → vocab → OMOP)
- **OMOP Tables:** 16 final tables
- **Incremental Models:** 1 (measurement)
- **Daily Refresh:** All dimension tables

### Target State
- **Total Models:** ~115 (10 fewer after consolidation)
- **Architecture:** Same 4 layers (optimized)
- **Incremental Models:** 6 (5 new fact tables)
- **Performance:** 70%+ improvement overall
- **Maintainability:** Simpler, cleaner code

## 🔍 Architecture Overview

```
models/
├── source/              # 33 VIEW models → ~23 after consolidation
│   ├── flex/           # Flex EHR (21 → ~15 models)
│   ├── swisslab/       # SwissLab (2 models)
│   ├── bi/             # BI system (2 models)
│   └── endoscopy/      # GI procedures (8 models)
├── staging/            # 23 FULL models
│   ├── flex/           # Flex-specific (14 models)
│   ├── swisslab/       # SwissLab-specific (2 models)
│   ├── bi/             # BI-specific (1 model)
│   └── common/         # System-agnostic (6 models)
├── vocab/              # 16 VIEW models → +5 concept map views
│   └── vocab__*        # OMOP concept mapping
└── omop/               # 16 models (15 FULL → 10 FULL + 6 INCREMENTAL)
```

## 🎓 Key Concepts

### Incremental Loading
**What:** Load only new/changed data instead of full refresh  
**Benefit:** 60-80% faster for large fact tables  
**Strategy:** `INCREMENTAL_BY_TIME_RANGE` with `updated_at` column

### Model Consolidation
**What:** Combine similar source models with type discriminators  
**Benefit:** Fewer models, simpler architecture  
**Example:** 3 care_site models → 1 with `care_site_type` column

### Vocab Optimization
**What:** Pre-filter concept mappings by domain  
**Benefit:** Simpler queries, 30-50% faster  
**Approach:** Create dedicated concept map views per domain

## 🛠️ Tools & Commands

### Generate DAG Visualization
```bash
sqlmesh dag person.html --select-model +lth_bronze.person+
sqlmesh dag measurement.html --select-model +lth_bronze.measurement+
sqlmesh dag observation.html --select-model +lth_bronze.observation+
```

### Performance Benchmarking
```bash
# Time model execution
sqlmesh evaluate lth_bronze.{model_name} --timing

# Validate row counts
sqlmesh fetchdf "SELECT COUNT(*) FROM lth_bronze.{table_name}"
```

### Testing
```bash
# Run all tests
sqlmesh test

# Run specific test
sqlmesh test --match "test_person"

# Run audits
sqlmesh audit --model lth_bronze.person
```

## 📞 Support

For questions or clarifications:
- **Technical Lead:** [TBD]
- **Project Owner:** LTH DST
- **Documentation:** This folder

## 📝 Document History

| Date | Version | Changes |
|------|---------|---------|
| 2026-02-14 | 1.0 | Initial optimization plan and subtasks created |

---

**Last Updated:** 2026-02-14  
**Status:** Ready for Implementation  
**Next Review:** After Phase 1 completion
