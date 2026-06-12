"""
Configuration settings for the E-Commerce ETL Pipeline.

Supports seamless switching between local development and cloud production.
Set the ENVIRONMENT variable in the .env file to 'local' or 'cloud'.

Authentication:
    Cloud mode uses Snowflake private key authentication (enterprise style).
    The private key file (.p8) is read and passed to SQLAlchemy via connect_args.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file (if it exists)
load_dotenv()

# ---------------------------------------------------------------------------
# Project Root & Environment Setup
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENVIRONMENT = os.environ.get("ENVIRONMENT", "local").lower()

# ---------------------------------------------------------------------------
# Dynamic Storage Configuration (Local Filesystem vs S3 Data Lake)
# ---------------------------------------------------------------------------
if ENVIRONMENT == "cloud":
    # Cloud: Use S3 bucket paths
    BASE_STORAGE_URI = os.environ.get("S3_BUCKET_URI", "s3://cv-minicapstone-blob-bucket").rstrip("/")
    BRONZE_PATH = f"{BASE_STORAGE_URI}/bronze"
    SILVER_PATH = f"{BASE_STORAGE_URI}/silver"
    GOLD_PATH = f"{BASE_STORAGE_URI}/gold"

    # Source file URIs (located in Bronze layer)
    WEBLOGS_FILE = f"{BRONZE_PATH}/weblogs.csv"
    USERS_FILE = f"{BRONZE_PATH}/users.csv"
    PRODUCTS_FILE = f"{BRONZE_PATH}/products.csv"
    
    BAD_RECORDS_WEBLOGS = f"{BRONZE_PATH}/bad_records_weblogs.csv"
    BAD_RECORDS = f"{BRONZE_PATH}/bad_records.csv"
else:
    # Local: Use local data directories
    BASE_STORAGE_URI = os.path.join(PROJECT_ROOT, "data")
    BRONZE_PATH = os.path.join(BASE_STORAGE_URI, "bronze")
    SILVER_PATH = os.path.join(BASE_STORAGE_URI, "silver")
    GOLD_PATH = os.path.join(BASE_STORAGE_URI, "gold")

    # Source file URIs (located in Bronze layer)
    WEBLOGS_FILE = os.path.join(BRONZE_PATH, "weblogs.csv")
    USERS_FILE = os.path.join(BRONZE_PATH, "users.csv")
    PRODUCTS_FILE = os.path.join(BRONZE_PATH, "products.csv")

    BAD_RECORDS_WEBLOGS = os.path.join(BRONZE_PATH, "bad_records_weblogs.csv")
    BAD_RECORDS = os.path.join(BRONZE_PATH, "bad_records.csv")

# ---------------------------------------------------------------------------
# Dynamic Database Configuration (SQLite vs Snowflake)
# ---------------------------------------------------------------------------

# Snowflake settings (read once, used by both URI and connect_args)
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER", "")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MINICAPSTONE_DB")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "WEATHER_API")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_PRIVATE_KEY_PATH = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "keys/rsa_key.p8")

if ENVIRONMENT == "cloud":
    # Cloud: Snowflake with private key authentication
    # The URI omits the password; the private key is injected via connect_args
    SQLALCHEMY_DATABASE_URI = (
        f"snowflake://{SNOWFLAKE_USER}@{SNOWFLAKE_ACCOUNT}"
        f"/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}"
        f"?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )

    # Build connect_args with the private key bytes
    SQLALCHEMY_CONNECT_ARGS = {}
    _key_path = os.path.join(PROJECT_ROOT, SNOWFLAKE_PRIVATE_KEY_PATH)
    if os.path.exists(_key_path):
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        with open(_key_path, "rb") as key_file:
            _private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # Assumes unencrypted .p8 key
                backend=default_backend(),
            )
        _private_key_bytes = _private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        SQLALCHEMY_CONNECT_ARGS = {"private_key": _private_key_bytes}
    else:
        import logging
        logging.getLogger("Settings").warning(
            f"Private key file not found at {_key_path}. "
            f"Snowflake authentication will fail at runtime."
        )
else:
    # Local: Build SQLite SQLAlchemy URI
    sqlite_path = os.path.join(PROJECT_ROOT, "ecommerce.db")
    if os.name == "nt":
        sqlite_path = sqlite_path.replace("\\", "/")
    SQLALCHEMY_DATABASE_URI = f"sqlite:///{sqlite_path}"
    SQLALCHEMY_CONNECT_ARGS = {}

# ---------------------------------------------------------------------------
# ETL Pipeline Settings
# ---------------------------------------------------------------------------
CHUNK_SIZE = 10_000   # Number of rows per chunk for CSV reading

# Threshold for flagging high-activity sessions
HIGH_ACTIVITY_THRESHOLD = 50

# Valid user actions
VALID_ACTIONS = ["view", "add_to_cart", "purchase"]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(name)-18s | %(levelname)-7s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ---------------------------------------------------------------------------
# Output files
# ---------------------------------------------------------------------------
DATA_QUALITY_REPORT_PATH = os.path.join(PROJECT_ROOT, "data_quality_report.md")
