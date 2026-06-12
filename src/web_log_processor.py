"""
WebLogProcessor — OOP ETL Pipeline

Implements the full Extract → Validate → Transform → Enrich → Load pipeline
using the Medallion Architecture (Bronze → Silver → Gold).

Architecture:
    - Data Lake: Amazon S3 (simulated with local directories)
    - Data Warehouse: Snowflake (simulated with SQLite)
"""

import os
import logging
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.exc import SQLAlchemyError
from src.connections import SnowflakeConnectionManager
from src.schema_managers import (
    DimUserTableManager,
    DimProductTableManager,
    FactUserActivityTableManager,
    AggSessionMetricsTableManager,
    DataQualityReportTableManager
)

from config.settings import (
    BRONZE_PATH,
    SILVER_PATH,
    GOLD_PATH,
    SQLALCHEMY_DATABASE_URI,
    SQLALCHEMY_CONNECT_ARGS,
    CHUNK_SIZE,
    HIGH_ACTIVITY_THRESHOLD,
    VALID_ACTIONS,
    LOG_LEVEL,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    DATA_QUALITY_REPORT_PATH,
    BAD_RECORDS,
    BAD_RECORDS_WEBLOGS,
)
from src.etl_helpers import (
    validate_schema,
    handle_missing_values,
    deduplicate,
    parse_timestamps,
    categorize_actions,
    compute_session_metrics,
    compute_conversion_rate,
    identify_abandoned_carts,
    flag_high_activity_sessions,
    detect_orphan_ids,
    detect_invalid_session_ids,
    clean_users_dimension,
    clean_products_dimension,
)
from src.data_quality import (
    DataQualityTracker,
    generate_data_quality_report,
)


class WebLogProcessor:
    """
    Full ETL pipeline for e-commerce web log analysis.

    Medallion Architecture:
        Bronze → raw CSV ingestion
        Silver → cleaned, validated, transformed data
        Gold   → enriched, aggregated data ready for analytics
    """

    # Expected schemas for validation
    WEBLOG_COLUMNS = ["log_id", "user_id", "product_id", "session_id", "action", "timestamp"]
    USER_COLUMNS = ["user_id", "user_name", "email", "signup_date"]
    PRODUCT_COLUMNS = ["product_id", "product_name", "category", "price"]

    def __init__(self, weblog_file: str, users_file: str, products_file: str):
        """
        Initialize the ETL pipeline.

        Args:
            weblog_file:  Path to weblogs CSV (in Bronze layer).
            users_file:   Path to users CSV (in Bronze layer).
            products_file: Path to products CSV (in Bronze layer).
        """
        self.weblog_file = weblog_file
        self.users_file = users_file
        self.products_file = products_file

        # DataFrames (populated during pipeline stages)
        self.users_df = None
        self.products_df = None
        self.weblogs_df = None
        self.enriched_df = None
        self.session_metrics_df = None

        # Database connection (SQLAlchemy Engine)
        self.engine = None

        # Data quality tracker
        self.tracker = DataQualityTracker()

        # Configure logging
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL),
            format=LOG_FORMAT,
            datefmt=LOG_DATE_FORMAT,
        )
        self.logger = logging.getLogger("WebLogProcessor")

    # ------------------------------------------------------------------
    # EXTRACT — Bronze Layer
    # ------------------------------------------------------------------
    def extract(self, chunk_size: int = CHUNK_SIZE):
        """
        Load CSV files from the Bronze layer (S3 data lake).
        Weblogs are read in chunks to handle large files efficiently,
        then concatenated for cross-chunk session analysis.
        """
        self.logger.info("=" * 60)
        self.logger.info("EXTRACT PHASE — Loading from Bronze Layer (S3)")
        self.logger.info("=" * 60)

        try:
            # Load users
            self.logger.info(f"Loading users from {self.users_file}")
            self.users_df = pd.read_csv(self.users_file)
            self.tracker.rows_extracted_users = len(self.users_df)
            self.logger.info(f"  -> {len(self.users_df):,} user records loaded.")

            # Load products
            self.logger.info(f"Loading products from {self.products_file}")
            self.products_df = pd.read_csv(self.products_file)
            self.tracker.rows_extracted_products = len(self.products_df)
            self.logger.info(f"  -> {len(self.products_df):,} product records loaded.")

            # Load weblogs in chunks
            self.logger.info(f"Loading weblogs from {self.weblog_file} (chunk_size={chunk_size:,})")
            chunks = []
            for i, chunk in enumerate(pd.read_csv(self.weblog_file, chunksize=chunk_size)):
                chunks.append(chunk)
                self.logger.info(f"  -> Chunk {i + 1}: {len(chunk):,} rows")

            if chunks:
                self.weblogs_df = pd.concat(chunks, ignore_index=True)
                self.tracker.rows_extracted_weblogs = len(self.weblogs_df)
                self.logger.info(
                    f"  -> Total: {len(self.weblogs_df):,} weblog records loaded "
                    f"across {len(chunks)} chunks."
                )
            else:
                self.weblogs_df = pd.DataFrame(columns=self.WEBLOG_COLUMNS)
                self.logger.warning("  -> Weblog file was empty.")

        except FileNotFoundError as e:
            self.logger.error(f"Extraction failed: Required file not found - {e.filename}")
            raise RuntimeError(f"Missing source file: {e.filename}") from e
        except pd.errors.EmptyDataError as e:
            self.logger.error(f"Extraction failed: A source file is empty and cannot be parsed - {e}")
            raise RuntimeError("Source file is empty") from e

    # ------------------------------------------------------------------
    # VALIDATE — Bronze → Silver transition
    # ------------------------------------------------------------------
    def validate(self):
        """
        Validate and clean extracted data. Detect anomalies and log them.
        """
        self.logger.info("=" * 60)
        self.logger.info("VALIDATE PHASE — Data Quality Checks")
        self.logger.info("=" * 60)

        # --- Schema Validation ---
        self.logger.info("Validating schemas...")
        for name, df, expected in [
            ("weblogs", self.weblogs_df, self.WEBLOG_COLUMNS),
            ("users", self.users_df, self.USER_COLUMNS),
            ("products", self.products_df, self.PRODUCT_COLUMNS),
        ]:
            result = validate_schema(df, expected)
            if not result["is_valid"]:
                msg = f"Schema issue in {name}: missing {result['missing_columns']}"
                self.tracker.schema_issues.append(msg)
                self.tracker.add_anomaly(msg)

        # --- Duplicate Detection (weblogs) ---
        self.logger.info("Detecting duplicates in weblogs...")
        self.weblogs_df, dup_count = deduplicate(self.weblogs_df, subset_columns=["log_id"])
        self.tracker.duplicate_rows_removed = dup_count

        # --- Validate and Clean Users Dimension ---
        self.logger.info("Cleaning users dimension...")
        before_users = len(self.users_df)
        self.users_df = clean_users_dimension(self.users_df)
        self.tracker.duplicate_rows_removed += (before_users - len(self.users_df))
        
        # Track specific user DQ issues
        self.tracker.duplicate_emails = int(self.users_df['duplicate_email_flag'].sum())
        self.tracker.invalid_emails = int(self.users_df['invalid_email_flag'].sum())
        # Replace missing user_name with UNKNOWN was handled in clean_users_dimension
        
        # --- Validate and Clean Products Dimension ---
        self.logger.info("Cleaning products dimension...")
        before_products = len(self.products_df)
        self.products_df = clean_products_dimension(self.products_df)
        self.tracker.duplicate_rows_removed += (before_products - len(self.products_df))
        
        # Create DataFrames to collect rejected records
        bad_records_weblogs_list = []
        bad_records_list = []

        # --- Duplicate Detection (weblogs) ---
        self.logger.info("Detecting duplicates in weblogs...")
        self.weblogs_df, dup_count = deduplicate(self.weblogs_df, subset_columns=["log_id"])
        self.tracker.duplicate_rows_removed += dup_count

        # --- Missing / Null user IDs ---
        self.logger.info("Checking for null user IDs...")
        null_user_mask = self.weblogs_df["user_id"].isna()
        null_user_count = int(null_user_mask.sum())
        self.tracker.null_user_ids = null_user_count
        if null_user_count > 0:
            bad_records_weblogs_list.append(self.weblogs_df[null_user_mask].assign(reject_reason="Missing user_id"))
            self.weblogs_df = self.weblogs_df[~null_user_mask]
        
        # Convert user_id to int after dropping nulls
        self.weblogs_df["user_id"] = self.weblogs_df["user_id"].astype(int)

        # --- Invalid Session IDs ---
        self.logger.info("Detecting invalid session IDs...")
        invalid_sessions, invalid_sess_count = detect_invalid_session_ids(self.weblogs_df)
        self.tracker.null_session_ids = int(self.weblogs_df["session_id"].isna().sum())
        self.tracker.malformed_session_ids = invalid_sess_count - self.tracker.null_session_ids
        if invalid_sess_count > 0:
            bad_records_weblogs_list.append(invalid_sessions.assign(reject_reason="Invalid session_id"))
            self.weblogs_df = self.weblogs_df.drop(invalid_sessions.index)

        # --- Timestamp Validation ---
        self.logger.info("Parsing and validating timestamps...")
        self.weblogs_df, invalid_ts = parse_timestamps(self.weblogs_df, "timestamp")
        self.tracker.invalid_timestamps = invalid_ts

        # Drop rows with invalid timestamps
        invalid_ts_mask = self.weblogs_df["timestamp"].isna()
        if invalid_ts > 0:
            bad_records_weblogs_list.append(self.weblogs_df[invalid_ts_mask].assign(reject_reason="Invalid timestamp"))
            self.weblogs_df = self.weblogs_df[~invalid_ts_mask]

        # --- Orphan Product IDs ---
        self.logger.info("Detecting orphan product IDs...")
        orphan_products = detect_orphan_ids(self.weblogs_df, self.products_df, "product_id")
        self.tracker.orphan_product_ids = len(orphan_products)
        if len(orphan_products) > 0:
            bad_records_list.append(orphan_products.assign(reject_reason="Orphan product_id"))
            self.weblogs_df = self.weblogs_df.drop(orphan_products.index)

        # --- Orphan User IDs ---
        self.logger.info("Detecting orphan user IDs...")
        orphan_users = detect_orphan_ids(self.weblogs_df, self.users_df, "user_id")
        self.tracker.orphan_user_ids = len(orphan_users)
        if len(orphan_users) > 0:
            bad_records_list.append(orphan_users.assign(reject_reason="Orphan user_id"))
            self.weblogs_df = self.weblogs_df.drop(orphan_users.index)

        # --- Action Categorization ---
        self.logger.info("Validating action categories...")
        self.weblogs_df, invalid_act_count = categorize_actions(
            self.weblogs_df, "action", VALID_ACTIONS
        )
        self.tracker.invalid_actions = invalid_act_count

        self.tracker.rows_after_cleaning = len(self.weblogs_df)
        
        # Save bad records
        if bad_records_weblogs_list:
            bad_weblogs_df = pd.concat(bad_records_weblogs_list, ignore_index=True)
            bad_weblogs_df.to_csv(BAD_RECORDS_WEBLOGS, index=False)
            self.logger.info(f"Saved {len(bad_weblogs_df)} bad weblogs to {BAD_RECORDS_WEBLOGS}")
            
        if bad_records_list:
            bad_records_df = pd.concat(bad_records_list, ignore_index=True)
            bad_records_df.to_csv(BAD_RECORDS, index=False)
            self.logger.info(f"Saved {len(bad_records_df)} orphan records to {BAD_RECORDS}")

        self.logger.info(f"Validation complete. {len(self.weblogs_df):,} clean rows remain.")

    # ------------------------------------------------------------------
    # TRANSFORM — Silver Layer
    # ------------------------------------------------------------------
    def transform(self):
        """
        Apply business logic transformations and compute session metrics.
        Save cleaned data to the Silver layer.
        """
        self.logger.info("=" * 60)
        self.logger.info("TRANSFORM PHASE — Silver Layer")
        self.logger.info("=" * 60)

        if self.weblogs_df is None or self.weblogs_df.empty:
            self.logger.warning("No weblogs to transform. Skipping transform phase.")
            self.session_metrics_df = pd.DataFrame()
            return

        # Sort logs by timestamp per session (handle out-of-order logs)
        self.logger.info("Sorting logs by timestamp per session...")
        self.weblogs_df = self.weblogs_df.sort_values(
            ["session_id", "timestamp"]
        ).reset_index(drop=True)

        # Compute session metrics (vectorized with NumPy)
        self.logger.info("Computing session metrics (vectorized)...")
        self.session_metrics_df = compute_session_metrics(self.weblogs_df)

        if not self.session_metrics_df.empty:
            # Conversion rates
            self.session_metrics_df = compute_conversion_rate(self.session_metrics_df)

            # Abandoned carts
            self.session_metrics_df = identify_abandoned_carts(self.session_metrics_df)
            self.tracker.abandoned_carts = int(
                self.session_metrics_df["is_abandoned_cart"].sum()
            )

            # High-activity sessions
            self.session_metrics_df = flag_high_activity_sessions(
                self.session_metrics_df, threshold=HIGH_ACTIVITY_THRESHOLD
            )
            self.tracker.high_activity_sessions = int(
                self.session_metrics_df["is_high_activity"].sum()
            )

            # Check for negative durations
            neg_dur = self.session_metrics_df["duration_seconds"] < 0
            self.tracker.negative_durations = int(neg_dur.sum())
            if self.tracker.negative_durations:
                self.tracker.add_anomaly(
                    f"Found {self.tracker.negative_durations} sessions with negative durations."
                )

            self.tracker.sessions_computed = len(self.session_metrics_df)

        # Save cleaned weblogs to Silver layer
        if SILVER_PATH.startswith("s3://"):
            silver_weblogs = f"{SILVER_PATH}/weblogs_cleaned.csv"
        else:
            silver_weblogs = os.path.join(SILVER_PATH, "weblogs_cleaned.csv")
        self.weblogs_df.to_csv(silver_weblogs, index=False)
        self.logger.info(f"Saved cleaned weblogs to Silver: {silver_weblogs}")

        # Save session metrics to Silver layer
        if not self.session_metrics_df.empty:
            if SILVER_PATH.startswith("s3://"):
                silver_metrics = f"{SILVER_PATH}/session_metrics.csv"
            else:
                silver_metrics = os.path.join(SILVER_PATH, "session_metrics.csv")
            self.session_metrics_df.to_csv(silver_metrics, index=False)
            self.logger.info(f"Saved session metrics to Silver: {silver_metrics}")

    # ------------------------------------------------------------------
    # ENRICH — Silver → Gold transition
    # ------------------------------------------------------------------
    def enrich(self):
        """
        Merge weblogs with user and product dimensions.
        Save enriched data to the Gold layer.
        """
        self.logger.info("=" * 60)
        self.logger.info("ENRICH PHASE — Gold Layer")
        self.logger.info("=" * 60)

        if self.weblogs_df is None or self.weblogs_df.empty:
            self.logger.warning("No weblogs to enrich. Skipping enrich phase.")
            self.enriched_df = pd.DataFrame()
            return

        # Merge logs with users (left join — keep all logs even if user is orphan)
        self.logger.info("Merging weblogs with users...")
        self.enriched_df = self.weblogs_df.merge(
            self.users_df, on="user_id", how="left", suffixes=("", "_user")
        )

        # Merge with products (left join)
        self.logger.info("Merging with products...")
        self.enriched_df = self.enriched_df.merge(
            self.products_df, on="product_id", how="left", suffixes=("", "_product")
        )

        self.logger.info(f"Enriched dataset: {len(self.enriched_df):,} rows, {len(self.enriched_df.columns)} columns.")

        # Save enriched data and final tables to Gold layer
        def get_gold_path(filename):
            if GOLD_PATH.startswith("s3://"):
                return f"{GOLD_PATH}/{filename}"
            return os.path.join(GOLD_PATH, filename)

        # 1. Enriched logs
        gold_enriched = get_gold_path("enriched_logs.csv")
        self.enriched_df.to_csv(gold_enriched, index=False)
        self.logger.info(f"Saved enriched logs to Gold: {gold_enriched}")

        # 2. Dim User
        if self.users_df is not None:
            gold_user = get_gold_path("dim_user.csv")
            self.users_df.to_csv(gold_user, index=False)
            self.logger.info(f"Saved dim_user to Gold: {gold_user}")

        # 3. Dim Product
        if self.products_df is not None:
            gold_product = get_gold_path("dim_product.csv")
            self.products_df.to_csv(gold_product, index=False)
            self.logger.info(f"Saved dim_product to Gold: {gold_product}")

        # 4. Fact User Activity (Cleaned Weblogs before aggregations)
        if self.weblogs_df is not None:
            gold_fact = get_gold_path("fact_user_activity.csv")
            self.weblogs_df.to_csv(gold_fact, index=False)
            self.logger.info(f"Saved fact_user_activity to Gold: {gold_fact}")

        if self.session_metrics_df is not None and not self.session_metrics_df.empty:
            if GOLD_PATH.startswith("s3://"):
                gold_metrics = f"{GOLD_PATH}/session_metrics_final.csv"
            else:
                gold_metrics = os.path.join(GOLD_PATH, "session_metrics_final.csv")
            self.session_metrics_df.to_csv(gold_metrics, index=False)
            self.logger.info(f"Saved final session metrics to Gold: {gold_metrics}")

    # ------------------------------------------------------------------
    # LOAD — Gold → Snowflake (SQLite locally)
    # ------------------------------------------------------------------
    def load(self, engine):
        """
        Load dimension and fact tables into the database using Modular Schema Managers.
        """
        self.logger.info("=" * 60)
        self.logger.info("LOAD PHASE — Loading into Database (Enterprise OOP)")
        self.logger.info("=" * 60)

        self.engine = engine

        try:
            # Dim User
            self.logger.info("Managing dim_user...")
            user_mgr = DimUserTableManager(self.engine)
            user_mgr.create_table()
            user_mgr.load_data(self.users_df)
            if self.users_df is not None:
                self.tracker.rows_loaded_dim_user = len(self.users_df)

            # Dim Product
            self.logger.info("Managing dim_product...")
            prod_mgr = DimProductTableManager(self.engine)
            prod_mgr.create_table()
            prod_mgr.load_data(self.products_df)
            if self.products_df is not None:
                self.tracker.rows_loaded_dim_product = len(self.products_df)

            # Fact User Activity
            self.logger.info("Managing fact_user_activity...")
            fact_mgr = FactUserActivityTableManager(self.engine)
            fact_mgr.create_table()
            fact_mgr.load_data(self.weblogs_df)
            fact_mgr.create_indexes()
            fact_mgr.validate()
            if self.weblogs_df is not None:
                self.tracker.rows_loaded_fact = len(self.weblogs_df)

            # Agg Session Metrics
            self.logger.info("Managing agg_session_metrics...")
            agg_mgr = AggSessionMetricsTableManager(self.engine)
            agg_mgr.create_table()
            if self.session_metrics_df is not None:
                agg_mgr.load_data(self.session_metrics_df)
                self.tracker.rows_loaded_agg = len(self.session_metrics_df)
            agg_mgr.create_indexes()
            agg_mgr.validate()

            # Audit Log / Data Quality Report
            self.logger.info("Managing data_quality_report...")
            audit_mgr = DataQualityReportTableManager(self.engine)
            audit_mgr.create_table()
            audit_mgr.load_data(self.tracker)

            self.logger.info("Load phase complete.")
        except SQLAlchemyError as e:
            self.logger.error(f"Database error during load phase: {e}")
            raise RuntimeError(f"Database load failed: {e}") from e

    # ------------------------------------------------------------------
    # RUN — Full Pipeline Orchestration
    # ------------------------------------------------------------------
    def run(self):
        """
        Execute the full ETL pipeline:
            Extract -> Validate -> Transform -> Enrich -> Load
        """
        self.logger.info("*" * 60)
        self.logger.info("  E-COMMERCE ETL PIPELINE — STARTING")
        self.logger.info("  Medallion Architecture: Bronze -> Silver -> Gold")
        self.logger.info("*" * 60)

        current_stage = "Initialization"
        try:
            current_stage = "Extract"
            self.extract()
            
            current_stage = "Validate"
            self.validate()
            
            current_stage = "Transform"
            self.transform()
            
            current_stage = "Enrich"
            self.enrich()

            current_stage = "Load"
            # Connect to database via OOP Connection Manager
            db_manager = SnowflakeConnectionManager()
            engine = db_manager.connect()
            self.load(engine)

            current_stage = "Reporting"
            # Generate data quality report
            generate_data_quality_report(self.tracker, DATA_QUALITY_REPORT_PATH)
            
            # Save data quality report to Gold layer as CSV
            report_records = self.tracker.generate_report_records()
            if report_records:
                report_df = pd.DataFrame(report_records)
                if GOLD_PATH.startswith("s3://"):
                    gold_report = f"{GOLD_PATH}/data_quality_report.csv"
                else:
                    gold_report = os.path.join(GOLD_PATH, "data_quality_report.csv")
                report_df.to_csv(gold_report, index=False)
                self.logger.info(f"Saved data_quality_report to Gold: {gold_report}")

            self.logger.info("*" * 60)
            self.logger.info("  ETL PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("*" * 60)

            # Print summary
            self._print_summary()

        except Exception as e:
            self.logger.error(f"Pipeline failed during {current_stage} stage: {e}", exc_info=True)
            raise
        finally:
            if self.engine:
                self.engine.dispose()

    # ------------------------------------------------------------------
    # Private Helpers
    # ------------------------------------------------------------------
    def _print_summary(self):
        """Print a formatted summary of the ETL run."""
        s = self.tracker.summary()
        print("\n" + "=" * 60)
        print("  ETL PIPELINE SUMMARY")
        print("=" * 60)
        print(f"  Run Timestamp:          {s['run_timestamp']}")
        print(f"  Rows Extracted (total):  {s['rows_extracted_total']:,}")
        print(f"    - Users:               {s['rows_extracted_users']:,}")
        print(f"    - Products:            {s['rows_extracted_products']:,}")
        print(f"    - Weblogs:             {s['rows_extracted_weblogs']:,}")
        print(f"  Rows After Cleaning:     {s['rows_after_cleaning']:,}")
        print(f"  Duplicates Removed:      {s['duplicate_rows_removed']:,}")
        print(f"  Invalid Timestamps:      {s['invalid_timestamps']:,}")
        print(f"  Null User IDs:           {s['null_user_ids']:,}")
        print(f"  Orphan User IDs:         {s['orphan_user_ids']:,}")
        print(f"  Orphan Product IDs:      {s['orphan_product_ids']:,}")
        print(f"  Sessions Computed:       {s['sessions_computed']:,}")
        print(f"  Abandoned Carts:         {s['abandoned_carts']:,}")
        print(f"  High-Activity Sessions:  {s['high_activity_sessions']:,}")
        print(f"  Rows Loaded (total):     {s['rows_loaded_total']:,}")
        print(f"  Anomalies:               {len(s['anomalies'])}")
        print("=" * 60)
