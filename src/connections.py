"""
Database and Storage Connection Managers
Enterprise OOP Architecture for Reusability
"""

import os
import boto3
import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# In a real enterprise app, you might not import settings directly here if you want 
# pure decoupling, but for this project we'll import the configured URIs.
from config.settings import SQLALCHEMY_DATABASE_URI, SQLALCHEMY_CONNECT_ARGS

logger = logging.getLogger("Connections")

class S3ConnectionManager:
    """Manages connections to AWS S3."""
    
    def __init__(self):
        self.aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.region = os.environ.get("AWS_REGION", "us-east-1")
        self.client = None

    def connect(self):
        """Initializes the boto3 S3 client."""
        if not self.aws_access_key or not self.aws_secret_key:
            logger.warning("AWS credentials not found. S3 connection may fail if required.")
            
        try:
            self.client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.region,
            )
            logger.info(f"Connected to S3 (region: {self.region}).")
            return self.client
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            raise

class SnowflakeConnectionManager:
    """Manages SQLAlchemy connections to Snowflake (or SQLite local surrogate)."""
    
    def __init__(self, uri: str = SQLALCHEMY_DATABASE_URI, connect_args: dict = SQLALCHEMY_CONNECT_ARGS):
        self.uri = uri
        self.connect_args = connect_args
        self.engine: Engine | None = None

    def connect(self) -> Engine:
        """Creates and returns the SQLAlchemy engine."""
        try:
            self.engine = create_engine(
                self.uri,
                connect_args=self.connect_args
            )
            logger.info("Database engine created successfully.")
            return self.engine
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise

    def dispose(self):
        """Disposes the engine connections."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database engine disposed.")
