"""
Star Schema Table Managers
Enterprise OOP Architecture for Modularity
"""

import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from src.data_quality import DataQualityTracker
from src.models import Base, DimUser, DimProduct, FactUserActivity, AggSessionMetrics, DataQualityReport

logger = logging.getLogger("SchemaManagers")

class BaseTableManager:
    """Base class for all table managers."""
    def __init__(self, engine: Engine):
        self.engine = engine

    def execute_query(self, query: str):
        """Executes a raw SQL query."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(query))
        except SQLAlchemyError as e:
            logger.error(f"Error executing query: {e}")
            raise

    def clean_records(self, records: list) -> list:
        """Cleans records for database insertion, handling NaN and dialect-specific timestamp requirements."""
        import pandas as pd
        from datetime import datetime
        for record in records:
            for k, v in record.items():
                if pd.isnull(v):
                    record[k] = None
                elif isinstance(v, pd.Timestamp):
                    if self.engine.dialect.name == 'sqlite':
                        record[k] = v.to_pydatetime()
                    else:
                        record[k] = v.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(v, datetime):
                    if self.engine.dialect.name != 'sqlite':
                        record[k] = v.strftime('%Y-%m-%d %H:%M:%S')
        return records


class DimUserTableManager(BaseTableManager):
    """Manages the dim_user dimension table using ORM."""
    
    def create_table(self):
        # Drop table if exists to allow schema updates
        DimUser.__table__.drop(self.engine, checkfirst=True)
        # Using Base metadata to create table
        Base.metadata.create_all(self.engine, tables=[DimUser.__table__])

    def load_data(self, df: pd.DataFrame):
        """Truncates and reloads the dimension table using ORM bulk inserts."""
        if not df.empty:
            self.execute_query("DELETE FROM dim_user;") # SQLite surrogate for TRUNCATE
            # Convert timestamp column back to datetime if it's string
            if 'signup_date' in df.columns:
                df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')
            
            records = df.to_dict(orient="records")
            records = self.clean_records(records)
            
            with Session(self.engine) as session:
                session.bulk_insert_mappings(DimUser, records)
                session.commit()
            logger.info(f"Loaded {len(df)} rows into dim_user via ORM.")


class DimProductTableManager(BaseTableManager):
    """Manages the dim_product dimension table using ORM."""
    
    def create_table(self):
        DimProduct.__table__.drop(self.engine, checkfirst=True)
        Base.metadata.create_all(self.engine, tables=[DimProduct.__table__])

    def load_data(self, df: pd.DataFrame):
        if not df.empty:
            self.execute_query("DELETE FROM dim_product;")
            records = df.to_dict(orient="records")
            with Session(self.engine) as session:
                session.bulk_insert_mappings(DimProduct, records)
                session.commit()
            logger.info(f"Loaded {len(df)} rows into dim_product via ORM.")


class FactUserActivityTableManager(BaseTableManager):
    """Manages the fact_user_activity fact table using ORM."""
    
    def create_table(self):
        FactUserActivity.__table__.drop(self.engine, checkfirst=True)
        Base.metadata.create_all(self.engine, tables=[FactUserActivity.__table__])

    def load_data(self, df: pd.DataFrame):
        if df is not None and not df.empty:
            fact_columns = ["log_id", "user_id", "product_id", "session_id", "action", "timestamp"]
            fact_df = df[fact_columns].copy()
            fact_df['timestamp'] = pd.to_datetime(fact_df['timestamp'], errors='coerce')
            
            self.execute_query("DELETE FROM fact_user_activity;")
            
            records = fact_df.to_dict(orient="records")
            records = self.clean_records(records)
            with Session(self.engine) as session:
                session.bulk_insert_mappings(FactUserActivity, records)
                session.commit()
            logger.info(f"Loaded {len(fact_df)} rows into fact_user_activity via ORM.")

    def create_indexes(self):
        """Create indexes for performance optimization."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_fact_user_id ON fact_user_activity(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_fact_session_id ON fact_user_activity(session_id);",
            "CREATE INDEX IF NOT EXISTS idx_fact_timestamp ON fact_user_activity(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_fact_action ON fact_user_activity(action);"
        ]
        for idx in indexes:
            try:
                self.execute_query(idx)
            except Exception as e:
                logger.warning(f"Index creation skipped (likely Snowflake): {e}")
        logger.info("Index creation logic complete for fact_user_activity.")

    def validate(self):
        """Data quality validation against the database."""
        # Check for orphan session IDs (if there was a session dimension)
        pass


class AggSessionMetricsTableManager(BaseTableManager):
    """Manages the agg_session_metrics aggregated table using ORM."""
    
    def create_table(self):
        AggSessionMetrics.__table__.drop(self.engine, checkfirst=True)
        Base.metadata.create_all(self.engine, tables=[AggSessionMetrics.__table__])

    def load_data(self, df: pd.DataFrame):
        if df is not None and not df.empty:
            df = df.copy()
            df['session_start'] = pd.to_datetime(df['session_start'], errors='coerce')
            df['session_end'] = pd.to_datetime(df['session_end'], errors='coerce')
            
            self.execute_query("DELETE FROM agg_session_metrics;")
            
            records = df.to_dict(orient="records")
            records = self.clean_records(records)
            with Session(self.engine) as session:
                session.bulk_insert_mappings(AggSessionMetrics, records)
                session.commit()
            logger.info(f"Loaded {len(df)} rows into agg_session_metrics via ORM.")

    def create_indexes(self):
        """Create indexes or partition logic."""
        # Simulated partitioning by indexing session_start
        try:
            self.execute_query("CREATE INDEX IF NOT EXISTS idx_agg_session_start ON agg_session_metrics(session_start);")
            self.execute_query("CREATE INDEX IF NOT EXISTS idx_agg_user_id ON agg_session_metrics(user_id);")
            logger.info("Indexes created on agg_session_metrics.")
        except Exception as e:
            logger.warning(f"Index creation skipped (likely Snowflake): {e}")

    def validate(self):
        """SQL validation for Data Quality."""
        try:
            with self.engine.connect() as conn:
                # Check for negative durations
                res = conn.execute(text("SELECT COUNT(*) FROM agg_session_metrics WHERE duration_seconds < 0;"))
                count = res.scalar()
                if count > 0:
                    logger.warning(f"Data Quality Alert: Found {count} sessions with negative durations in database.")
                else:
                    logger.info("Validation passed: No negative durations found in database.")
        except SQLAlchemyError as e:
            logger.error(f"Validation failed: {e}")


class DataQualityReportTableManager(BaseTableManager):
    """Manages the data_quality_report system table using ORM."""
    
    def create_table(self):
        DataQualityReport.__table__.drop(self.engine, checkfirst=True)
        Base.metadata.create_all(self.engine, tables=[DataQualityReport.__table__])

    def load_data(self, tracker: DataQualityTracker):
        """Appends new data quality records to the log."""
        reports = tracker.generate_report_records()
        if not reports:
            return
            
        reports = self.clean_records(reports)
        
        with Session(self.engine) as session:
            session.bulk_insert_mappings(DataQualityReport, reports)
            session.commit()
        logger.info(f"Appended {len(reports)} data quality report records via ORM.")
