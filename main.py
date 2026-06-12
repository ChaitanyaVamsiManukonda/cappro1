"""
Main Entry Point — E-Commerce ETL Pipeline

Runs the full Extract → Validate → Transform → Enrich → Load pipeline
using the Medallion Architecture (Bronze → Silver → Gold).

Usage:
    python main.py
"""

import os
import sys

# Parse command line argument for environment mode (local/cloud)
mode = "local"
if len(sys.argv) > 1:
    arg = sys.argv[1].lower()
    if arg in ["local", "cloud"]:
        mode = arg
    else:
        print(f"Warning: Unknown mode '{arg}'. Using default 'local'.")

# Set the environment variable BEFORE importing config/settings
os.environ["ENVIRONMENT"] = mode

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import WEBLOGS_FILE, USERS_FILE, PRODUCTS_FILE
from src.web_log_processor import WebLogProcessor

def main():
    """Instantiate and run the ETL pipeline."""
    print("=" * 60)
    print("  Capstone Project Team 1 — E-Commerce ETL Pipeline")
    print("  Architecture: Medallion (Bronze -> Silver -> Gold)")
    print("  Data Lake: S3 (simulated locally)")
    print("  Data Warehouse: Snowflake (simulated via SQLite)")
    print("=" * 60)
    print()

    # Verify source files exist
    for label, path in [("Weblogs", WEBLOGS_FILE), ("Users", USERS_FILE), ("Products", PRODUCTS_FILE)]:
        if not path.startswith("s3://") and not os.path.exists(path):
            print(f"ERROR: {label} file not found at: {path}")
            print("Please run generate_data.py first to create sample data.")
            sys.exit(1)

    # Create and run the pipeline
    processor = WebLogProcessor(
        weblog_file=WEBLOGS_FILE,
        users_file=USERS_FILE,
        products_file=PRODUCTS_FILE,
    )
    processor.run()


if __name__ == "__main__":
    main()
