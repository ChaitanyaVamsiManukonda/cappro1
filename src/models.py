from sqlalchemy import Column, Integer, String, Float, Boolean, TIMESTAMP, text
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class DimUser(Base):
    __tablename__ = 'dim_user'
    
    user_id = Column(Integer, primary_key=True)
    user_name = Column(String(255))
    email = Column(String(255))
    signup_date = Column(TIMESTAMP)
    duplicate_email_flag = Column(Integer)
    invalid_email_flag = Column(Integer)

class DimProduct(Base):
    __tablename__ = 'dim_product'
    
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String(255))
    category = Column(String(100))
    price = Column(Float)

class FactUserActivity(Base):
    __tablename__ = 'fact_user_activity'
    
    log_id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    product_id = Column(Integer)
    session_id = Column(String(100))
    action = Column(String(50))
    timestamp = Column(TIMESTAMP)

class AggSessionMetrics(Base):
    __tablename__ = 'agg_session_metrics'
    
    session_id = Column(String(100), primary_key=True)
    user_id = Column(Integer)
    session_start = Column(TIMESTAMP)
    session_end = Column(TIMESTAMP)
    total_actions = Column(Integer)
    total_views = Column(Integer)
    total_purchases = Column(Integer)
    total_add_to_cart = Column(Integer)
    unique_products = Column(Integer)
    duration_seconds = Column(Float)
    conversion_rate = Column(Float)
    is_abandoned_cart = Column(Boolean)
    is_high_activity = Column(Boolean)

class DataQualityReport(Base):
    __tablename__ = 'data_quality_report'
    
    # SQLite requires a primary key, Snowflake doesn't strictly require one but it's good practice
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(100))
    issue_type = Column(String(255))
    record_count = Column(Integer)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
