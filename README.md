# E-Commerce Web Log ETL Pipeline — Capstone Project Team 1

## Overview

A Python-based ETL pipeline that analyzes e-commerce user behavior from web logs. The pipeline extracts raw CSV data, cleans and validates it, computes session-level metrics, and loads enriched data into a star schema for business intelligence analytics.

## Architecture

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Lake** | Amazon S3 (simulated locally) | Raw data storage |
| **ETL Engine** | Python (Pandas + NumPy) | Data processing |
| **Data Warehouse** | Snowflake (simulated via SQLite) | Analytical queries |
| **Design Pattern** | Medallion Architecture | Bronze → Silver → Gold |

### Medallion Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│   BRONZE (Raw)          SILVER (Cleaned)       GOLD (Enriched)   │
│   ┌─────────────┐      ┌─────────────┐       ┌──────────────┐   │
│   │ users.csv   │──►   │ weblogs_    │──►    │ enriched_    │   │
│   │ products.csv│      │  cleaned.csv│       │  logs.csv    │   │
│   │ weblogs.csv │      │ session_    │       │ session_     │   │
│   │             │      │  metrics.csv│       │  metrics_    │   │
│   │ (S3 Bucket) │      │             │       │  final.csv   │   │
│   └─────────────┘      └─────────────┘       └──────┬───────┘   │
│                                                      │           │
│                                                      ▼           │
│                                              ┌──────────────┐    │
│                                              │  Snowflake   │    │
│                                              │  (SQLite)    │    │
│                                              │              │    │
│                                              │ dim_user     │    │
│                                              │ dim_product  │    │
│                                              │ fact_user_   │    │
│                                              │  activity    │    │
│                                              │ agg_session_ │    │
│                                              │  metrics     │    │
│                                              │ etl_audit_log│    │
│                                              └──────────────┘    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
cap_pro1/
├── config/
│   └── settings.py              # Configuration (paths, DB, constants)
├── data/
│   ├── bronze/                  # Raw CSVs (S3 landing zone)
│   ├── silver/                  # Cleaned & validated data
│   └── gold/                    # Enriched & aggregated data
├── docs/
│   ├── capstone_project_team_1_requirements_xml.md
│   └── erd_star_schema.md       # Star Schema ERD (Mermaid)
├── sql/
│   └── analytics/bi_queries.sql # 10 BI analytical queries
├── src/
│   ├── connections.py           # OOP S3 and Snowflake connection managers
│   ├── schema_managers.py       # OOP Star Schema table managers (DDL/Validations)
│   ├── etl_helpers.py           # Functional programming helpers
│   ├── web_log_processor.py     # OOP WebLogProcessor class
│   └── data_quality.py          # Auditing & reporting
├── tests/
│   └── test_transformations.py  # Unit tests
├── generate_data.py             # Sample data generator
├── main.py                      # Entry point
├── requirements.txt             # Dependencies
└── README.md                    # This file
```

## How to Run

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate Sample Data (if not already present)
```bash
python generate_data.py
```

### 3. Run the ETL Pipeline
```bash
python main.py
```

### 4. Run Unit Tests
```bash
python -m pytest tests/ -v
```

## Pipeline Stages

| Stage | Method | Description |
|-------|--------|-------------|
| **Extract** | `extract()` | Load CSVs in chunks from Bronze layer |
| **Validate** | `validate()` | Schema checks, dedup, null handling, orphan detection |
| **Transform** | `transform()` | Parse timestamps, compute session metrics (NumPy vectorized) |
| **Enrich** | `enrich()` | Merge logs with user & product dimensions |
| **Load** | `load()` | Insert into star schema tables (Snowflake/SQLite) |

## Key Design Decisions

1. **Local-First Execution**: Fully runnable locally using local file paths (simulating S3) and SQLite (simulating Snowflake). Switching to real cloud services requires only configuration changes.

2. **Chunked Reading**: Weblogs are read in configurable chunks (default 10,000 rows) to handle large files efficiently. Chunks are concatenated before session grouping to ensure accurate cross-chunk metrics.

3. **Vectorized Operations**: All metric computations use NumPy/Pandas vectorized operations — no Python loops — for performance.

4. **Comprehensive Auditing**: Every pipeline run produces a `data_quality_report.md` and inserts an audit record into `etl_audit_log`.

5. **Null User IDs**: Filled with `-1` (not dropped) to preserve log entries for session and product analysis.

## SQL Analytics

The `sql/analytics/bi_queries.sql` file contains 10 BI queries:

1. Most viewed products
2. Conversion rates (purchase sessions / engagement sessions)
3. Average session duration
4. Top users by total purchases
5. Abandoned cart rate per day/week
6. Product conversion rate (purchases / views)
7. Average number of actions per session
8. Peak activity hours
9. Sessions with unusually long durations
10. Cohort analysis (signup month vs purchases)

Plus 3 data quality validation queries.

## Assumptions

- User IDs with `None` are filled with `-1` to retain the log entry for product/session analysis.
- Invalid timestamps (`invalid_timestamp`) are coerced to `NaT` and the corresponding rows are dropped (session metrics require valid datetimes).
- Session metrics are computed per `session_id` — logs with null session IDs are excluded from session aggregation but kept in the fact table.
- Duplicate detection for weblogs uses `log_id` as the unique identifier.
- The SQLite database acts as a local surrogate for Snowflake. The DDL, DML, and table validations are handled purely in Python via `src/schema_managers.py` for enterprise reusability.
