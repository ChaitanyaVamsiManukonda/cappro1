"""
Data Quality & Auditing Module

Provides:
    - DataQualityTracker: accumulates ETL metrics throughout the pipeline run.
    - save_audit_log(): persists the audit record to the etl_audit_log table.
    - generate_data_quality_report(): writes a human-readable Markdown report.
"""

import logging
from datetime import datetime

logger = logging.getLogger("DataQuality")


class DataQualityTracker:
    """Tracks ETL quality metrics across pipeline stages."""

    def __init__(self):
        self.run_timestamp = datetime.now().isoformat()

        # Extraction metrics
        self.rows_extracted_users = 0
        self.rows_extracted_products = 0
        self.rows_extracted_weblogs = 0

        # Validation metrics
        self.duplicate_rows_removed = 0
        self.invalid_timestamps = 0
        self.null_user_ids = 0
        self.null_session_ids = 0
        self.malformed_session_ids = 0
        self.orphan_user_ids = 0
        self.orphan_product_ids = 0
        self.invalid_actions = 0
        self.schema_issues = []
        
        # New Specific Data Quality Rules metrics
        self.duplicate_emails = 0
        self.invalid_emails = 0

        # Transformation metrics
        self.rows_after_cleaning = 0
        self.sessions_computed = 0
        self.abandoned_carts = 0
        self.high_activity_sessions = 0

        # Load metrics
        self.rows_loaded_dim_user = 0
        self.rows_loaded_dim_product = 0
        self.rows_loaded_fact = 0
        self.rows_loaded_agg = 0

        # Anomalies
        self.negative_durations = 0
        self.anomalies = []

    @property
    def total_rows_extracted(self) -> int:
        return (
            self.rows_extracted_users
            + self.rows_extracted_products
            + self.rows_extracted_weblogs
        )

    @property
    def total_rows_loaded(self) -> int:
        return (
            self.rows_loaded_dim_user
            + self.rows_loaded_dim_product
            + self.rows_loaded_fact
            + self.rows_loaded_agg
        )

    @property
    def total_invalid_rows(self) -> int:
        return (
            self.duplicate_rows_removed
            + self.invalid_timestamps
            + self.null_user_ids
            + self.null_session_ids
            + self.malformed_session_ids
            + self.invalid_actions
        )

    def add_anomaly(self, description: str):
        self.anomalies.append(description)
        logger.warning(f"Anomaly recorded: {description}")

    def summary(self) -> dict:
        """Return a summary dictionary of all metrics."""
        return {
            "run_timestamp": self.run_timestamp,
            "rows_extracted_total": self.total_rows_extracted,
            "rows_extracted_users": self.rows_extracted_users,
            "rows_extracted_products": self.rows_extracted_products,
            "rows_extracted_weblogs": self.rows_extracted_weblogs,
            "duplicate_rows_removed": self.duplicate_rows_removed,
            "invalid_timestamps": self.invalid_timestamps,
            "null_user_ids": self.null_user_ids,
            "null_session_ids": self.null_session_ids,
            "orphan_user_ids": self.orphan_user_ids,
            "orphan_product_ids": self.orphan_product_ids,
            "rows_after_cleaning": self.rows_after_cleaning,
            "sessions_computed": self.sessions_computed,
            "abandoned_carts": self.abandoned_carts,
            "high_activity_sessions": self.high_activity_sessions,
            "negative_durations": self.negative_durations,
            "rows_loaded_total": self.total_rows_loaded,
            "rows_loaded_dim_user": self.rows_loaded_dim_user,
            "rows_loaded_dim_product": self.rows_loaded_dim_product,
            "rows_loaded_fact": self.rows_loaded_fact,
            "rows_loaded_agg": self.rows_loaded_agg,
            "total_invalid_rows": self.total_invalid_rows,
            "anomalies": self.anomalies,
            "duplicate_emails": self.duplicate_emails,
            "invalid_emails": self.invalid_emails,
        }

    def generate_report_records(self) -> list:
        """Generate list of dicts for the data_quality_report table."""
        from datetime import datetime
        now = datetime.utcnow()
        records = []
        
        mapping = {
            "Duplicate User IDs": self.duplicate_rows_removed, # approximation based on usage
            "Duplicate Emails": self.duplicate_emails,
            "Invalid Emails": self.invalid_emails,
            "Invalid Sessions": self.malformed_session_ids,
            "Orphan Products": self.orphan_product_ids,
            "Orphan Users": self.orphan_user_ids,
            "Invalid Timestamps": self.invalid_timestamps,
            "Missing User IDs in Logs": self.null_user_ids
        }
        
        for issue_type, count in mapping.items():
            if count > 0:
                records.append({
                    "run_id": self.run_timestamp,
                    "issue_type": issue_type,
                    "record_count": count,
                    "created_at": now
                })
        return records

import pandas as pd
from sqlalchemy.engine import Engine

def save_audit_log(tracker: DataQualityTracker, engine: Engine):
    """
    Deprecated: Replaced by DataQualityReportTableManager
    """
    pass


def generate_data_quality_report(tracker: DataQualityTracker, output_path: str):
    """
    Generate a human-readable Markdown data quality report.
    """
    s = tracker.summary()

    report = f"""# Data Quality Report
**Run Timestamp:** {s['run_timestamp']}

---

## Extraction Summary
| Metric | Count |
|--------|------:|
| Users extracted | {s['rows_extracted_users']:,} |
| Products extracted | {s['rows_extracted_products']:,} |
| Weblogs extracted | {s['rows_extracted_weblogs']:,} |
| **Total rows extracted** | **{s['rows_extracted_total']:,}** |

## Validation & Cleaning
| Issue | Count |
|-------|------:|
| Duplicate rows removed | {s['duplicate_rows_removed']:,} |
| Invalid timestamps | {s['invalid_timestamps']:,} |
| Null user IDs | {s['null_user_ids']:,} |
| Null/malformed session IDs | {s['null_session_ids']:,} |
| Orphan user IDs (in logs, not in users) | {s['orphan_user_ids']:,} |
| Orphan product IDs (in logs, not in products) | {s['orphan_product_ids']:,} |
| **Total invalid/skipped rows** | **{s['total_invalid_rows']:,}** |
| **Rows after cleaning** | **{s['rows_after_cleaning']:,}** |

## Transformation Summary
| Metric | Count |
|--------|------:|
| Sessions computed | {s['sessions_computed']:,} |
| Abandoned cart sessions | {s['abandoned_carts']:,} |
| High-activity sessions (>50 actions) | {s['high_activity_sessions']:,} |
| Negative duration sessions | {s['negative_durations']:,} |

## Load Summary
| Table | Rows Loaded |
|-------|------------:|
| dim_user | {s['rows_loaded_dim_user']:,} |
| dim_product | {s['rows_loaded_dim_product']:,} |
| fact_user_activity | {s['rows_loaded_fact']:,} |
| agg_session_metrics | {s['rows_loaded_agg']:,} |
| **Total rows loaded** | **{s['rows_loaded_total']:,}** |

## Anomalies
"""
    if s["anomalies"]:
        for i, anomaly in enumerate(s["anomalies"], 1):
            report += f"{i}. {anomaly}\n"
    else:
        report += "No anomalies detected.\n"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)

    logger.info(f"Data quality report written to {output_path}")
