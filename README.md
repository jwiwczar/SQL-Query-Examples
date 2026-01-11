# SQL Portfolio – Real Production Queries (Sanitized)

Hi! I'm Jessica Wiwczar, a self-taught SQL enthusiast who spent 2+ years building complex queries in a fast-paced biotech environment.

This repo showcases three real production PostgreSQL queries I wrote and maintained (2023–2025). They solved actual team problems like batch QC reporting, modular assembly analysis, and sequence extraction.

All queries are **sanitized** (generalized table/column names for confidentiality) but preserve original logic, performance choices, and domain flavor.

## Queries Included

| # | File | Description | Key Techniques |
|---|------|-------------|----------------|
| 1 | queries/assay_results_sanitized.sql | Comprehensive QC dashboard for multiple samples (yields, purity, stability, etc.) | Early filtering for perf win, ROW_NUMBER ranking, wide joins, CASE corrections |
| 2 | queries/binders_sql_sanitized.sql | Analyze process compositions with multiple components (Fab/Fc types, linkers) | Nested CTEs per component, JSONB handling, orientation filtering |
| 3 | queries/cdrs_sql_sanitized.sql | Extract CDR sequences for entities in one row | CTE for chain filtering, COALESCE-safe concatenation |

## Why These Queries?
- Real-world messiness: Repetitions, quirks, and optimizations I chose not to "AI-refactor" because they worked best.
- Performance story: Pushing filters early sped things up dramatically.
- Domain insight: Biotech workflows (processes, entities, chains) shine through without proprietary details.

## Notes
- Schema is proprietary → no DDL or data here. Happy to discuss patterns or walk through live!
- All run on PostgreSQL; uses {{parameters}} for flexibility (e.g., in tools like dbt or BI).

Thanks for checking it out! Feel free to fork or reach out on LinkedIn.
