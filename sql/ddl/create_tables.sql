-- ============================================================================
-- Snowflake DDL — E-Commerce ETL Pipeline
-- Medallion Architecture: Gold Layer (Data Warehouse)
--
-- Tables:
--   dim_user             — User dimension
--   dim_product          — Product dimension
--   fact_user_activity   — Granular web log events (fact table)
--   agg_session_metrics  — Pre-aggregated session-level metrics
--   etl_audit_log        — ETL pipeline audit trail
-- ============================================================================

-- ---------------------------------------------------------------------------
-- DIMENSION: dim_user
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_user (
    user_id     INTEGER     PRIMARY KEY,
    user_name   VARCHAR(200),
    email       VARCHAR(200),
    signup_date DATE
);

COMMENT ON TABLE dim_user IS 'User dimension table — contains user profile data.';


-- ---------------------------------------------------------------------------
-- DIMENSION: dim_product
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_product (
    product_id   INTEGER      PRIMARY KEY,
    product_name VARCHAR(200),
    category     VARCHAR(100),
    price        DECIMAL(10, 2)
);

COMMENT ON TABLE dim_product IS 'Product dimension table — contains product catalog data.';


-- ---------------------------------------------------------------------------
-- FACT: fact_user_activity
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE fact_user_activity (
    log_id      INTEGER,
    user_id     INTEGER     REFERENCES dim_user(user_id),
    product_id  INTEGER     REFERENCES dim_product(product_id),
    session_id  VARCHAR(50),
    action      VARCHAR(20),
    timestamp   TIMESTAMP,

    CONSTRAINT pk_fact_user_activity PRIMARY KEY (log_id)
);

COMMENT ON TABLE fact_user_activity IS
    'Fact table — one row per web log event (view, add_to_cart, purchase).';

-- Indexes for query performance
CREATE OR REPLACE INDEX idx_fact_user_id    ON fact_user_activity(user_id);
CREATE OR REPLACE INDEX idx_fact_session_id ON fact_user_activity(session_id);
CREATE OR REPLACE INDEX idx_fact_timestamp  ON fact_user_activity(timestamp);
CREATE OR REPLACE INDEX idx_fact_action     ON fact_user_activity(action);


-- ---------------------------------------------------------------------------
-- AGGREGATE: agg_session_metrics
-- Partitioned by date for Snowflake performance (clustering key)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE agg_session_metrics (
    session_id        VARCHAR(50) PRIMARY KEY,
    user_id           INTEGER,
    session_start     TIMESTAMP,
    session_end       TIMESTAMP,
    duration_seconds  DECIMAL(12, 2),
    total_actions     INTEGER,
    total_views       INTEGER,
    total_purchases   INTEGER,
    total_add_to_cart INTEGER,
    unique_products   INTEGER,
    conversion_rate   DECIMAL(5, 4),
    is_abandoned_cart BOOLEAN,
    is_high_activity  BOOLEAN
)
CLUSTER BY (TO_DATE(session_start));

COMMENT ON TABLE agg_session_metrics IS
    'Pre-aggregated session-level metrics. Clustered by session date for fast date-range queries.';

CREATE OR REPLACE INDEX idx_agg_user_id ON agg_session_metrics(user_id);


-- ---------------------------------------------------------------------------
-- AUDIT: etl_audit_log
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE etl_audit_log (
    audit_id          INTEGER     AUTOINCREMENT PRIMARY KEY,
    run_timestamp     TIMESTAMP,
    rows_extracted    INTEGER,
    rows_transformed  INTEGER,
    rows_loaded       INTEGER,
    rows_invalid      INTEGER,
    session_anomalies INTEGER,
    notes             TEXT
);

COMMENT ON TABLE etl_audit_log IS
    'ETL pipeline audit trail — one row per pipeline execution.';
