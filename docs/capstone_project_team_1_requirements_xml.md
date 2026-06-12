```xml
<?xml version="1.0" encoding="UTF-8"?>
<project>
    <team>Capstone Project Team 1</team>
    
    <scenario>
        <description>An e-commerce platform wants to analyze user behavior from web logs.</description>
        <task>Students will design a Python-based ETL pipeline, clean and parse log data, enrich it with user and product info, and create analytical tables for user behavior, conversion rates, and session analysis.</task>
    </scenario>
    
    <architecture_requirements>
        <data_lake>USE ONLY Amazon S3</data_lake>
        <rds>USE ONLY Snowflake</rds>
        <design_pattern>Use Medallion Architecture (Bronze, Silver, Gold layers)</design_pattern>
    </architecture_requirements>
    
    <objectives>
        <objective>Parse large log files in chunks</objective>
        <objective>Clean and normalize web logs</objective>
        <objective>Build fact and dimension tables</objective>
        <objective>Enable behavioral and conversion analysis</objective>
    </objectives>
    
    <etl_phases>
        <phase name="Extract" medallion_layer="Bronze" technologies="Python + Pandas">
            <description>Raw data should be stored in and extracted from the S3 Data Lake.</description>
            <core_tasks>
                <task>Load CSV files in chunks using Pandas to handle large files.</task>
                <task>Validate schema: Missing columns, Invalid data types</task>
                <task>Handle missing or null user IDs.</task>
                <task>Detect duplicates in log entries.</task>
            </core_tasks>
            <additional_tasks>
                <task>Detect orphan product IDs (log entries for missing products).</task>
                <task>Detect invalid session IDs (null or malformed).</task>
                <task>Log all anomalies to a data quality report.</task>
            </additional_tasks>
        </phase>
        
        <phase name="Transform" medallion_layer="Silver" technologies="Data Modeling &amp; Business Logic">
            <core_transformations>
                <transformation>Parse timestamps into datetime objects.</transformation>
                <transformation>Categorize user actions: view, add_to_cart, purchase</transformation>
                <transformation>Compute session duration using NumPy: session_end - session_start per session ID per user</transformation>
                <transformation>Aggregate metrics per session: Total actions, Total products viewed/purchased</transformation>
                <transformation>Merge logs with users and products for enrichment.</transformation>
            </core_transformations>
            <advanced_transformations>
                <transformation>Compute conversion rate per session: (purchases/views)</transformation>
                <transformation>Identify abandoned carts: sessions with add_to_cart but no purchase</transformation>
                <transformation>Flag high-activity sessions (e.g., &gt; 50 actions in a single session)</transformation>
                <transformation>Handle out-of-order logs: sort by timestamp per session</transformation>
                <transformation>Vectorize session duration computation using NumPy</transformation>
            </advanced_transformations>
        </phase>

        <phase name="Engineering_Requirements" technologies="Python">
            <paradigm name="Functional Programming">
                <requirement>Write reusable Python functions for: Schema validation, Missing value handling, Deduplication, Metric calculations</requirement>
            </paradigm>
            <paradigm name="OOP Design">
                <requirement>Create a class: WebLogProcessor with methods extract, validate, transform, enrich, load, run</requirement>
            </paradigm>
            <additional_tasks>
                <task>Create helper functions for parsing timestamps, categorizing actions, computing session metrics</task>
                <task>Implement logging for ETL steps and errors</task>
                <task>Handle corrupt rows gracefully</task>
                <task>Track ETL metrics (rows processed, rows skipped, anomalies)</task>
                <task>Implement unit tests for key transformations</task>
            </additional_tasks>
        </phase>
        
        <phase name="Load" medallion_layer="Gold" technologies="SQL + Database Design">
            <description>Data loading should ONLY target Snowflake as the RDS.</description>
            <database_tables>
                <table>dim_user</table>
                <table>dim_product</table>
                <table>fact_user_activity (logs per session/action)</table>
                <table>agg_session_metrics (aggregated session-level metrics)</table>
            </database_tables>
            <additional_tasks>
                <task>Define primary and foreign keys</task>
                <task>Add indexes on: [Columns to be determined]</task>
                <task>Upsert logic for dimension tables</task>
            </additional_tasks>
        </phase>
    </etl_phases>
    
    <sql_analytics location="Snowflake">
        <queries>
            <query>Most viewed products</query>
            <query>Conversion rates (sessions with purchase / sessions with view/add_to_cart)</query>
            <query>Average session duration</query>
            <query>Top users by total purchases</query>
            <query>Abandoned cart rate per day/week</query>
            <query>Product conversion rate (purchase/views)</query>
            <query>Average number of actions per session</query>
            <query>Peak activity hours on the site</query>
            <query>Sessions with unusually long durations</query>
            <query>Cohort analysis: users by signup month vs purchases</query>
        </queries>
    </sql_analytics>
    
    <data_quality_and_auditing>
        <python>
            <task>Track ETL metrics: Rows extracted, transformed, loaded; Invalid/missing rows; Session anomalies</task>
            <task>Save audit log to a SQL table etl_audit_log (in Snowflake)</task>
        </python>
        <sql location="Snowflake">
            <validation>No orphan session IDs</validation>
            <validation>No duplicate log entries</validation>
            <validation>Session durations &gt;= 0</validation>
        </sql>
    </data_quality_and_auditing>
    
    <performance_and_optimization>
        <python>Use vectorized operations (no loops)</python>
        <sql>
            <task>Analyze query plans</task>
            <task>Index frequently queried columns</task>
            <task>Partition aggregated session metrics by date</task>
        </sql>
    </performance_and_optimization>
    
    <deliverables>
        <deliverable>Python ETL code</deliverable>
        <deliverable>SQL DDL &amp; DML scripts</deliverable>
        <deliverable>Analytical SQL queries</deliverable>
        <deliverable>Data quality report</deliverable>
        <deliverable>ERD / Star Schema diagram</deliverable>
        <deliverable>README explaining architecture &amp; assumptions</deliverable>
    </deliverables>
    
    <data_generation_script language="python">
        <code><![CDATA[
import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
np.random.seed(42)

# -----------------------------
# Users
# -----------------------------
users = []
for i in range(1, 1101):
    users.append({
        "user_id": i if random.random() > 0.03 else random.randint(1, 50),  # duplicates
        "user_name": fake.name() if random.random() > 0.05 else None,
        "email": fake.email() if random.random() > 0.1 else "invalid_email",
        "signup_date": fake.date_between(start_date="-5y", end_date="today")
        if random.random() > 0.05 else "invalid_date"
    })
users_df = pd.DataFrame(users)
users_df.to_csv("users.csv", index=False)

# -----------------------------
# Products
# -----------------------------
categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]
products = []
for i in range(1, 1101):
    products.append({
        "product_id": i if random.random() > 0.02 else random.randint(1, 100),
        "product_name": fake.word().title(),
        "category": random.choice(categories),
        "price": round(random.uniform(5, 500), 2)
        if random.random() > 0.1 else None
    })
products_df = pd.DataFrame(products)
products_df.to_csv("products.csv", index=False)

# -----------------------------
# Web Logs
# -----------------------------
actions = ["view", "add_to_cart", "purchase"]
weblogs = []
for i in range(1, 15001):
    user_id = random.randint(1, 1200)  # include orphans
    product_id = random.randint(1, 1200)  # include orphans
    session_id = f"sess_{random.randint(1,5000)}" if random.random() > 0.05 else None
    timestamp = datetime.now() - timedelta(days=random.randint(0, 365),      seconds=random.randint(0,86400))
    if random.random() < 0.05:
        timestamp = "invalid_timestamp"

    weblogs.append({
        "log_id": i if random.random() > 0.03 else random.randint(1, 200),
        "user_id": user_id if random.random() > 0.05 else None,
        "product_id": product_id,
        "session_id": session_id,
        "action": random.choice(actions),
        "timestamp": timestamp
    })

weblogs_df = pd.DataFrame(weblogs)
weblogs_df.to_csv("weblogs.csv", index=False)

print("Web log CSV files generated successfully!")
        ]]></code>
    </data_generation_script>
</project>
```
