"""
ETL Helper Functions — Functional Programming Module

Provides reusable, pure functions for:
    - Schema validation
    - Missing value handling
    - Deduplication
    - Timestamp parsing
    - Action categorization
    - Session metric computation (vectorized with NumPy)
    - Conversion rate calculation
    - Abandoned cart identification
    - High-activity session flagging
    - Orphan ID detection
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("ETL_Helpers")


# ---------------------------------------------------------------------------
# Schema Validation
# ---------------------------------------------------------------------------
def validate_schema(df: pd.DataFrame, expected_columns: list) -> dict:
    """
    Validate that a DataFrame contains the expected columns.

    Returns:
        dict with keys:
            'is_valid'        (bool)
            'missing_columns' (list of str)
            'extra_columns'   (list of str)
    """
    actual = set(df.columns)
    expected = set(expected_columns)
    missing = expected - actual
    extra = actual - expected

    result = {
        "is_valid": len(missing) == 0,
        "missing_columns": sorted(missing),
        "extra_columns": sorted(extra),
    }

    if missing:
        logger.warning(f"Schema validation failed — missing columns: {missing}")
    else:
        logger.info("Schema validation passed.")

    return result


# ---------------------------------------------------------------------------
# Missing Value Handling
# ---------------------------------------------------------------------------
def handle_missing_values(
    df: pd.DataFrame,
    column: str,
    strategy: str = "drop",
    fill_value=None,
) -> tuple[pd.DataFrame, int]:
    """
    Handle missing/null values in a specific column.

    Args:
        df:         Input DataFrame.
        column:     Column name to process.
        strategy:   'drop' to remove rows, 'fill' to impute.
        fill_value: Value used when strategy='fill'.

    Returns:
        (processed DataFrame, count of rows affected)
    """
    null_count = int(df[column].isna().sum())

    if strategy == "drop":
        df = df.dropna(subset=[column])
    elif strategy == "fill":
        df[column] = df[column].fillna(fill_value)
    else:
        raise ValueError(f"Unknown strategy: {strategy!r}. Use 'drop' or 'fill'.")

    if null_count:
        logger.info(f"Handled {null_count} missing values in '{column}' (strategy={strategy}).")

    return df, null_count


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
def deduplicate(df: pd.DataFrame, subset_columns: list | None = None) -> tuple[pd.DataFrame, int]:
    """
    Remove duplicate rows.

    Returns:
        (deduplicated DataFrame, number of duplicates removed)
    """
    before = len(df)
    df = df.drop_duplicates(subset=subset_columns)
    dup_count = before - len(df)

    if dup_count:
        logger.info(f"Removed {dup_count} duplicate rows (subset={subset_columns}).")

    return df, dup_count


# ---------------------------------------------------------------------------
# Timestamp Parsing
# ---------------------------------------------------------------------------
def parse_timestamps(df: pd.DataFrame, column: str) -> tuple[pd.DataFrame, int]:
    """
    Parse a column into datetime, coercing invalid values to NaT.

    Returns:
        (DataFrame with parsed timestamps, count of invalid timestamps)
    """
    df[column] = pd.to_datetime(df[column], errors="coerce")
    invalid_count = int(df[column].isna().sum())

    if invalid_count:
        logger.info(f"Found {invalid_count} invalid timestamps in '{column}'.")

    return df, invalid_count


# ---------------------------------------------------------------------------
# Action Categorization
# ---------------------------------------------------------------------------
def categorize_actions(df: pd.DataFrame, column: str, valid_actions: list) -> tuple[pd.DataFrame, int]:
    """
    Validate that action values are within the allowed set.
    Rows with invalid actions are flagged (not removed) by adding an
    'is_valid_action' boolean column.

    Returns:
        (DataFrame with 'is_valid_action' column, count of invalid actions)
    """
    df["is_valid_action"] = df[column].isin(valid_actions)
    invalid_count = int((~df["is_valid_action"]).sum())

    if invalid_count:
        logger.warning(f"Found {invalid_count} rows with invalid actions in '{column}'.")

    return df, invalid_count


# ---------------------------------------------------------------------------
# Session Metrics — Vectorized with NumPy
# ---------------------------------------------------------------------------
def compute_session_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute aggregated session-level metrics using vectorized operations.

    Expects columns: session_id, user_id, timestamp, action, product_id.

    Returns a DataFrame indexed by session_id with columns:
        user_id, session_start, session_end, duration_seconds,
        total_actions, total_views, total_purchases, total_add_to_cart
    """
    # Drop rows without session_id (already flagged)
    valid = df.dropna(subset=["session_id"]).copy()

    if valid.empty:
        logger.warning("No valid sessions to compute metrics for.")
        return pd.DataFrame()

    # Sort by session and timestamp for correct ordering
    valid = valid.sort_values(["session_id", "timestamp"])

    grouped = valid.groupby("session_id")

    metrics = grouped.agg(
        user_id=("user_id", "first"),
        session_start=("timestamp", "min"),
        session_end=("timestamp", "max"),
        total_actions=("action", "count"),
        total_views=("action", lambda x: (x == "view").sum()),
        total_purchases=("action", lambda x: (x == "purchase").sum()),
        total_add_to_cart=("action", lambda x: (x == "add_to_cart").sum()),
        unique_products=("product_id", "nunique"),
    )

    # Vectorized duration calculation using NumPy
    duration_td = (metrics["session_end"] - metrics["session_start"])
    metrics["duration_seconds"] = duration_td.values / np.timedelta64(1, "s")

    logger.info(f"Computed metrics for {len(metrics)} sessions.")
    return metrics.reset_index()


# ---------------------------------------------------------------------------
# Conversion Rate
# ---------------------------------------------------------------------------
def compute_conversion_rate(session_metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Compute conversion rate per session: purchases / views.
    Handles division by zero (sessions with 0 views → rate = 0.0).
    """
    session_metrics = session_metrics.copy()
    session_metrics["conversion_rate"] = np.where(
        session_metrics["total_views"] > 0,
        session_metrics["total_purchases"] / session_metrics["total_views"],
        0.0,
    )
    return session_metrics


# ---------------------------------------------------------------------------
# Abandoned Cart Detection
# ---------------------------------------------------------------------------
def identify_abandoned_carts(session_metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Flag sessions that have at least one add_to_cart but zero purchases.
    """
    session_metrics = session_metrics.copy()
    session_metrics["is_abandoned_cart"] = (
        (session_metrics["total_add_to_cart"] > 0)
        & (session_metrics["total_purchases"] == 0)
    )
    abandoned = int(session_metrics["is_abandoned_cart"].sum())
    logger.info(f"Identified {abandoned} abandoned cart sessions.")
    return session_metrics


# ---------------------------------------------------------------------------
# High-Activity Session Flagging
# ---------------------------------------------------------------------------
def flag_high_activity_sessions(
    session_metrics: pd.DataFrame, threshold: int = 50
) -> pd.DataFrame:
    """
    Flag sessions with more than `threshold` actions.
    """
    session_metrics = session_metrics.copy()
    session_metrics["is_high_activity"] = session_metrics["total_actions"] > threshold
    high = int(session_metrics["is_high_activity"].sum())
    logger.info(f"Flagged {high} high-activity sessions (threshold={threshold}).")
    return session_metrics


# ---------------------------------------------------------------------------
# Orphan ID Detection
# ---------------------------------------------------------------------------
def detect_orphan_ids(
    log_df: pd.DataFrame,
    ref_df: pd.DataFrame,
    join_column: str,
) -> pd.DataFrame:
    """
    Find IDs in log_df that are NOT present in ref_df.

    Returns:
        DataFrame of orphan rows from log_df.
    """
    ref_ids = set(ref_df[join_column].dropna().unique())
    log_ids = log_df[join_column].dropna()
    orphan_mask = ~log_ids.isin(ref_ids)
    orphans = log_df.loc[orphan_mask]

    logger.info(
        f"Detected {len(orphans)} orphan entries for '{join_column}' "
        f"(log has {log_ids.nunique()} unique IDs, reference has {len(ref_ids)})."
    )
    return orphans


# ---------------------------------------------------------------------------
# Invalid Session ID Detection
# ---------------------------------------------------------------------------
def detect_invalid_session_ids(df: pd.DataFrame, column: str = "session_id") -> tuple[pd.DataFrame, int]:
    """
    Detect null or malformed session IDs.
    A valid session ID must be non-null and match the pattern 'sess_<number>'.

    Returns:
        (DataFrame of invalid rows, count)
    """
    is_null = df[column].isna()
    is_malformed = ~df[column].astype(str).str.match(r"^sess_\d+$", na=True)
    invalid_mask = is_null | is_malformed
    invalid_rows = df.loc[invalid_mask]
    count = len(invalid_rows)

    if count:
        logger.info(f"Detected {count} invalid session IDs (null or malformed).")

    return invalid_rows, count

# ---------------------------------------------------------------------------
# Dimension Cleaners
# ---------------------------------------------------------------------------
import re

def clean_users_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean users dataframe according to DQ rules:
    - Keep latest record for duplicate user_id (sorted by signup_date)
    - Flag duplicate emails
    - Replace missing user_name with UNKNOWN
    - Flag invalid emails
    """
    df = df.copy()
    
    # 1. Missing user_name -> UNKNOWN
    df['user_name'] = df['user_name'].fillna("UNKNOWN")
    
    # 2. Duplicate user_id -> keep latest (last after sorting by signup_date)
    if 'signup_date' in df.columns:
        df['signup_date_parsed'] = pd.to_datetime(df['signup_date'], errors='coerce')
        df = df.sort_values('signup_date_parsed')
        df = df.drop(columns=['signup_date_parsed'])
    df = df.drop_duplicates(subset=['user_id'], keep='last')
    
    # 3. Flag duplicate emails
    df['duplicate_email_flag'] = df['email'].duplicated(keep=False).astype(int)
    
    # 4. Flag invalid emails
    email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    df['invalid_email_flag'] = (~df['email'].astype(str).str.match(email_pattern, na=False)).astype(int)
    
    logger.info("Users dimension cleaned according to DQ rules.")
    return df

def clean_products_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean products dataframe according to DQ rules:
    - Keep latest record for duplicate product_id (keep last)
    - Set missing price to 0
    """
    df = df.copy()
    
    # 1. Duplicate product_id -> keep latest
    df = df.drop_duplicates(subset=['product_id'], keep='last')
    
    # 2. Missing price -> 0
    df['price'] = df['price'].fillna(0)
    
    logger.info("Products dimension cleaned according to DQ rules.")
    return df
