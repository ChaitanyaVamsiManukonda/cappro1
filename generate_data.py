"""
Data Generation Script — E-Commerce Sample Data

Generates three CSV files with intentional data quality issues:
    - users.csv     (~1,100 rows) — duplicate IDs, null names, invalid emails/dates
    - products.csv  (~1,100 rows) — duplicate IDs, null prices
    - weblogs.csv   (~15,000 rows) — orphan IDs, null sessions, invalid timestamps

Output: Files are saved to data/bronze/ (Medallion Bronze layer).

Usage:
    python generate_data.py
"""

import os
import random

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
np.random.seed(42)

# Output directory (Bronze layer)
BRONZE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)


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
users_df.to_csv(os.path.join(BRONZE_DIR, "users.csv"), index=False)
print(f"Generated {len(users_df)} user records.")


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
products_df.to_csv(os.path.join(BRONZE_DIR, "products.csv"), index=False)
print(f"Generated {len(products_df)} product records.")


# -----------------------------
# Web Logs
# -----------------------------
actions = ["view", "add_to_cart", "purchase"]
weblogs = []
for i in range(1, 15001):
    user_id = random.randint(1, 1200)  # include orphans
    product_id = random.randint(1, 1200)  # include orphans
    session_id = f"sess_{random.randint(1,5000)}" if random.random() > 0.05 else None
    timestamp = datetime.now() - timedelta(
        days=random.randint(0, 365), seconds=random.randint(0, 86400)
    )
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
weblogs_df.to_csv(os.path.join(BRONZE_DIR, "weblogs.csv"), index=False)
print(f"Generated {len(weblogs_df)} weblog records.")

print(f"\nAll files saved to: {BRONZE_DIR}")
print("Web log CSV files generated successfully!")
