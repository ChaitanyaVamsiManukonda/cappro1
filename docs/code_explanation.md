# E-Commerce ETL Pipeline: Comprehensive Code Explanation

This document provides a detailed, step-by-step explanation of how the Python-based ETL pipeline implemented in this repository satisfies the requirements outlined in the `capstone_project_team_1_requirements_xml.md` document, including a complete breakdown of each file and its associated methods.

## 1. File and Method Explanations

### `main.py`
The entry point of the pipeline. It establishes the runtime environment, parses command-line arguments to determine if the pipeline should run locally or on the cloud (Snowflake/S3), loads environment variables via `python-dotenv`, and invokes the core orchestrator.
*   **`main()`**: The primary script execution block. Sets up paths and connections based on arguments, instantiates `WebLogProcessor`, and executes its `.run()` method.

### `src/web_log_processor.py`
Contains the core object-oriented orchestrator for the entire ETL pipeline.
*   **`class WebLogProcessor`**: Manages the ETL state, tracking variables, and delegates transformations to functional helpers.
    *   **`__init__()`**: Initializes dataframes, storage paths, database engines, and the `DataQualityTracker`.
    *   **`extract()`**: Reads raw CSV files from the Bronze layer (local or S3). Uses Pandas `chunksize` to iterate over large `weblogs.csv` files to prevent memory exhaustion.
    *   **`validate()`**: Enforces data quality rules. Calls helper functions to deduplicate rows, detect orphan IDs, parse timestamps safely, and handle missing values. Anomalous rows are quarantined.
    *   **`transform()`**: Applies business logic to the Silver layer. It sorts out-of-order logs by session/timestamp, delegates session metric computations (duration, conversions) to vectorized helpers, and identifies abandoned carts or high-activity sessions.
    *   **`enrich()`**: Merges the cleaned fact weblog data with user and product dimensional datasets to create a fully enriched dataset for the Gold layer.
    *   **`load()`**: Dispatches the fully processed data to relational database tables utilizing the OOP Table Managers, handling ORM bulk insertions and index creations.
    *   **`run()`**: The orchestrator method that sequentially calls `extract()`, `validate()`, `transform()`, `enrich()`, and `load()`.

### `src/etl_helpers.py`
A collection of pure functional programming methods used to manipulate and evaluate dataframes without managing state.
*   **`validate_schema()`**: Checks that expected columns exist in a dataframe and validates minimal data types.
*   **`handle_missing_values()`**: Removes rows containing `NaN` or `Null` in specified critical columns (like `session_id`).
*   **`deduplicate()`**: Uses `drop_duplicates` on specified subsets (like `log_id`) to ensure row uniqueness.
*   **`parse_timestamps()`**: Safely converts string timestamps into Pandas datetime objects, coercing invalid strings into `NaT`.
*   **`detect_orphans()`**: Identifies rows in a fact table (like logs) whose Foreign Key IDs (user/product) do not exist in the referenced dimension tables.
*   **`categorize_actions()`**: Flags logs with unrecognized web actions.
*   **`clean_users_dimension()` / `clean_products_dimension()`**: Handles specific dimension rules, such as identifying invalid emails, duplicate user identities, or resolving negative product prices.
*   **`compute_session_metrics()`**: A heavily optimized, vectorized NumPy/Pandas function that groups logs by `session_id` to calculate session start/end times, durations, total actions, and sums of specific events (views, purchases).
*   **`identify_abandoned_carts()`**: Filters the aggregated metrics to find sessions where a user added an item to a cart but the total purchases remained `0`.
*   **`flag_high_activity()`**: Identifies bot-like behavior by flagging sessions that exceed a defined threshold (e.g., >50 actions).

### `src/schema_managers.py`
Utilizes Object-Oriented Programming to isolate the database insertion logic for specific tables using SQLAlchemy.
*   **`class BaseTableManager`**: An abstract parent class providing shared capabilities.
    *   **`execute_query()`**: Runs raw SQL queries safely.
    *   **`clean_records()`**: An essential utility that cleans dictionary records before insertion. It handles `NaN`/`NaT` to `None` conversions and standardizes Python `datetime` formatting (ensuring Snowflake compatibility using strings, or SQLite compatibility using objects).
*   **`class DimUserTableManager` / `DimProductTableManager`**: 
    *   **`create_table()`**: Drops and recreates the dimensional tables from SQLAlchemy metadata.
    *   **`load_data()`**: Performs truncate/delete workflows and bulk inserts dictionaries into the DB.
*   **`class FactUserActivityTableManager` / `AggSessionMetricsTableManager`**:
    *   **`create_table()`** and **`load_data()`**: Handles insertion for the large operational and aggregated tables.
    *   **`create_indexes()`**: Generates secondary indices on critical columns (e.g., `user_id`, `session_start`) to speed up analytical querying.
    *   **`validate()`**: Executes post-load SQL queries to confirm data integrity (like ensuring no negative session durations exist in the DB).
*   **`class DataQualityReportTableManager`**: Automatically appends the extracted anomaly metrics into a dedicated Snowflake table.

### `src/models.py`
Contains the SQLAlchemy Object-Relational Mapping (ORM) classes defining the Star Schema.
*   **`DimUser`**, **`DimProduct`**: SQLAlchemy class representations of the dimension tables.
*   **`FactUserActivity`**: SQLAlchemy representation of the log actions, containing Foreign Keys to the dimensions.
*   **`AggSessionMetrics`**: SQLAlchemy representation of the rolled-up user behavior analytics.
*   **`DataQualityReport`**: SQLAlchemy representation of the audit logging table.

### `src/data_quality.py`
Provides state tracking for all pipeline anomalies.
*   **`class DataQualityTracker`**: Accumulates counts of duplicated rows, invalid schemas, orphans, and successful loads throughout the ETL lifecycle.
    *   **`summary()`**: Compiles all metric counters into a consolidated dictionary.
    *   **`generate_report_records()`**: Translates metric dictionary into rows suitable for insertion into the `DataQualityReport` database table.
*   **`generate_data_quality_report()`**: Takes the tracker output and formats it into an easy-to-read Markdown file (`data_quality_report.md`).

### `src/connections.py`
Handles database connectivity.
*   **`get_engine()`**: Returns a SQLAlchemy `Engine`. It establishes a connection to either a local SQLite file (for testing) or the global Snowflake account depending on the provided credentials and configuration.

---

## 2. Architectural Paradigms & Engineering Design

### 2.1 Medallion Architecture
The pipeline strictly adheres to the **Medallion Architecture**, progressing through three distinct data layers:
*   **Bronze (Raw Data):** Represents the Extract phase. Raw CSV files (`users.csv`, `products.csv`, `weblogs.csv`) are ingested from an Amazon S3 Data Lake (simulated locally via boto3/aiobotocore or local paths). Anomalous and rejected records are safely quarantined here.
*   **Silver (Cleaned & Transformed Data):** Represents the Validate and Transform phases. The data is deduplicated, cleaned, normalized, and evaluated for session metrics. 
*   **Gold (Enriched Analytical Data):** Represents the Enrich and Load phases. Weblogs are enriched with user and product information, then loaded into dimensional and fact tables designed for analytical querying.

## 3. Extract Phase (Bronze Layer)

### 3.1 Chunking Large Files
Handling large log files efficiently is a core requirement. In `web_log_processor.py`, the `extract()` method loads the `weblogs.csv` file using Pandas `chunksize=10000`. By iterating over the chunks and appending them to a list, memory utilization is kept in check, allowing the system to scale for massive log files without causing out-of-memory errors.

## 4. Validate Phase (Data Quality)

The `validate()` phase handles the rigorous data quality checks required before transformation. Anomalies are tracked using a dedicated `DataQualityTracker` object.
*   **Missing and Invalid IDs:** The pipeline aggressively cleans missing/null `user_id`s and drops `session_id`s that are null or malformed.
*   **Orphan Detection:** Weblogs containing `user_id`s or `product_id`s that do not exist in the source dimension files are isolated and flagged.
*   **Timestamp Parsing:** Timestamps are strictly parsed into valid datetime objects, quarantining rows with string errors like "invalid_timestamp".

All skipped or anomalous records are routed to "bad records" CSV files in the Bronze layer, preventing silent data loss while preserving pipeline integrity.

## 5. Transform Phase (Silver Layer)

The `transform()` phase generates the core session intelligence required for analytical use cases.
*   **Out-of-Order Logs:** Weblogs are grouped by `session_id` and explicitly sorted by their `timestamp` to fix out-of-order events.
*   **Vectorized Session Computing:** In `etl_helpers.py`, session durations are calculated by utilizing NumPy and Pandas vectorized operations (`groupby` max and min) rather than utilizing slow Python loops. 
*   **Metric Aggregations:** Total actions, viewed products, and purchased products are aggregated per session.
*   **Advanced Analytics:** Computes conversion rates, identifies abandoned carts, and flags high-activity sessions.

## 6. Enrich Phase (Gold Layer)

In the `enrich()` phase, the now-pristine weblogs are joined (merged) against the cleaned user and product dimensions. This produces an enriched, flat dataset containing all dimensions and metrics natively—facilitating immediate business analytics. The flattened CSV output is written to the local/S3 Gold storage layer.

## 7. Load Phase (Database & Snowflake Integration)

The `load()` method represents the culmination of the pipeline, loading the processed layers into an RDBMS structure, heavily targeting **Snowflake**.

### 7.1 Star Schema Implementation
The database logic relies on SQLAlchemy ORM to build out the necessary tables: `dim_user`, `dim_product`, `fact_user_activity`, and `agg_session_metrics`.

### 7.2 Resilient Database Loading
The `src/schema_managers.py` file includes dialect-aware logic for upserts, dictionary cleaning (dialect-specific timestamp normalization), and creation of secondary indices where supported by the target engine.

### 7.3 Data Quality Auditing
All anomaly metrics tracked during extraction and validation are pushed into the `data_quality_report` table. Additionally, an overarching Markdown report is generated locally, establishing a permanent audit log of the ETL run.
