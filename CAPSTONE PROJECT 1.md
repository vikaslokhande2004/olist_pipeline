━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA ENGINEERING — KODE-X
DAY 14 : CAPSTONE PROJECT
Real-World GCP Data Engineering Pipeline
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Project Title:
OLIST E-COMMERCE END-TO-END PIPELINE

You are a data engineer hired by a Brazilian
e-commerce company called Olist.
They have 9 raw CSV files containing
100,000+ orders, customers, products,
sellers, payments, and reviews.

Your job: build a production-grade
data pipeline from scratch — ingest,
clean, transform, analyse, and deliver
business reports. Then automate it.

This project covers everything in your
completed syllabus PLUS introduces
the pending job-ready topics:
Git, environment variables, config files,
shell scripting, PySpark performance,
and Airflow introduction.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATASETS — DOWNLOAD BEFORE YOU START
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PRIMARY DATASET — Olist Brazilian E-Commerce
Source: Kaggle (free account required)
URL:
https://www.kaggle.com/datasets/olistbr/
brazilian-ecommerce

Click "Download" — you get a zip file
containing 9 CSVs. Extract all of them.

Files you will use:
olist_orders_dataset.csv
  → 99,441 rows. Has NULL approved dates,
    cancelled orders, mixed statuses.

olist_order_items_dataset.csv
  → 112,650 rows. One order can have
    multiple items. Has price and freight.

olist_customers_dataset.csv
  → 99,441 rows. Has city/state/zip.
    Duplicate customer entries exist.

olist_products_dataset.csv
  → 32,951 rows. Has NULL category names,
    NULL dimensions and weights.

olist_sellers_dataset.csv
  → 3,095 rows. Has city/state/zip.

olist_order_reviews_dataset.csv
  → 99,224 rows. Has NULL review comments.
    Score from 1 to 5. Dirty text fields.

olist_order_payments_dataset.csv
  → 103,886 rows. Multiple payment types
    per order. Has installments data.

product_category_name_translation.csv
  → 71 rows. Maps Portuguese category
    names to English. Use for JOIN.

ALTERNATIVE (no Kaggle account):
Direct GitHub mirror — all 9 files:
https://github.com/dr-roger-data/
olist/tree/main/data

Individual file direct download example:
https://raw.githubusercontent.com/
dr-roger-data/olist/main/data/
olist_orders_dataset.csv

HEAVY PYSPARK PERFORMANCE DATASET:
NYC Yellow Taxi Jan 2023 (Parquet, 3M rows)
Direct download — no signup:
https://d37ci6vzurychx.cloudfront.net/
trip-data/yellow_tripdata_2023-01.parquet

Save this file as:
raw/nyc_taxi_jan2023.parquet
You will use it in Phase 4 only.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 1 — PROJECT SETUP
Topics: Git, venv, config.env,
        directory structure, .gitignore
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This is the Job Ready module in practice.
Every data engineering job starts here.

---

STEP 1.1 — Create project structure

mkdir olist_pipeline
cd olist_pipeline

mkdir -p raw processed reports logs
mkdir -p scripts notebooks sql
mkdir -p src/ingestion src/transform
mkdir -p src/analysis src/utils

tree olist_pipeline/
Verify the directory tree is correct.

---

STEP 1.2 — Git initialisation

This is version control.
Every change you make will be tracked.

cd olist_pipeline
git init

Create .gitignore to exclude large files
and sensitive config:

nano .gitignore

Add these lines inside .gitignore:
raw/
*.parquet
*.gz
*.zip
__pycache__/
*.pyc
.env
config.env
venv/
.DS_Store
logs/

git add .gitignore
git commit -m "Initial commit: project structure"

Now after every phase you will commit.
This is the professional workflow.

---

STEP 1.3 — Python virtual environment

python3 -m venv venv
source venv/bin/activate
(On Windows: venv\Scripts\activate)

Create requirements.txt:

nano requirements.txt

Add:
pyspark==3.5.0
pandas==2.1.0
numpy==1.26.0
pyarrow==14.0.0
sqlalchemy==2.0.0
python-dotenv==1.0.0
loguru==0.7.2

Install all:
pip install -r requirements.txt

Verify:
pip list | grep pyspark
pip list | grep pandas

---

STEP 1.4 — Environment variables (config.env)

In production, passwords and paths are
NEVER hardcoded in Python files.
They are stored in environment files.

Create config.env:

nano config.env

Add:
PROJECT_NAME=olist_pipeline
RAW_PATH=./raw
PROCESSED_PATH=./processed
REPORTS_PATH=./reports
LOGS_PATH=./logs
SPARK_APP_NAME=OlistPipeline
SPARK_MASTER=local[*]
LOG_LEVEL=INFO
GCP_PROJECT=your-gcp-project-id
GCP_BUCKET=gs://olist-data-bucket
BQ_DATASET=olist_analytics

Load config.env in Python like this:

from dotenv import load_dotenv
import os

load_dotenv("config.env")

RAW_PATH       = os.getenv("RAW_PATH")
PROCESSED_PATH = os.getenv("PROCESSED_PATH")
PROJECT_NAME   = os.getenv("PROJECT_NAME")
SPARK_MASTER   = os.getenv("SPARK_MASTER")

print(f"Project: {PROJECT_NAME}")
print(f"Raw path: {RAW_PATH}")

This is how every production pipeline
reads configuration. Never hardcode.

---

STEP 1.5 — Create a logger utility

Create: src/utils/logger.py

import logging
import os
from datetime import datetime

def get_logger(name: str) -> logging.Logger:
    log_dir = os.getenv("LOGS_PATH", "./logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(
        log_dir,
        f"{name}_{datetime.now()
        .strftime('%Y%m%d_%H%M%S')}.log"
    )

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s "
        "| %(name)s | %(message)s"
    )
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

This logger writes to file AND prints
to terminal simultaneously.
Import it in every script you write.

---

STEP 1.6 — Git commit Phase 1

git add .
git commit -m "Phase 1: Setup complete.
  Git, venv, config.env, logger utility."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 2 — LINUX DATA INGESTION
Topics: Shell scripting, file inspection,
        Linux process management, awk, grep
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This phase uses Linux to inspect and
validate all raw data before any Python
or PySpark code runs.
In production this is the first check.

---

STEP 2.1 — Copy datasets to raw/

Copy all downloaded Olist CSVs:
cp ~/Downloads/olist_*.csv raw/
cp ~/Downloads/product_category*.csv raw/
ls -lh raw/

---

STEP 2.2 — Write a shell script for
            data profiling

This is the new topic: SHELL SCRIPTING.
A shell script is a .sh file that runs
multiple Linux commands in sequence —
exactly like Python scripts but using
Linux commands.

Create: scripts/profile_raw_data.sh

nano scripts/profile_raw_data.sh

Add this content:

#!/bin/bash
# ============================================
# profile_raw_data.sh
# Purpose: Profile all raw CSV files before
#          pipeline execution
# Usage: ./scripts/profile_raw_data.sh
# ============================================

set -euo pipefail
# set -e → exit immediately on any error
# set -u → treat unset variables as error
# set -o pipefail → catch errors in pipes

RAW_DIR="./raw"
LOG_DIR="./logs"
REPORT="$LOG_DIR/raw_profile_$(date +%Y%m%d_%H%M%S).txt"

mkdir -p "$LOG_DIR"

echo "======================================" | tee "$REPORT"
echo "RAW DATA PROFILE REPORT"               | tee -a "$REPORT"
echo "Generated: $(date)"                    | tee -a "$REPORT"
echo "======================================" | tee -a "$REPORT"

for file in "$RAW_DIR"/*.csv; do
    filename=$(basename "$file")
    echo ""                                  | tee -a "$REPORT"
    echo "File: $filename"                   | tee -a "$REPORT"
    echo "--------------------------------------" | tee -a "$REPORT"

    # Total lines including header
    total_lines=$(wc -l < "$file")
    data_rows=$((total_lines - 1))
    echo "Total rows (excl header): $data_rows" | tee -a "$REPORT"

    # Column count from header
    col_count=$(head -1 "$file" | tr ',' '\n' | wc -l)
    echo "Column count: $col_count"          | tee -a "$REPORT"

    # File size
    size=$(du -sh "$file" | cut -f1)
    echo "File size: $size"                  | tee -a "$REPORT"

    # Header row
    echo "Columns: $(head -1 "$file")"       | tee -a "$REPORT"

    # Count empty/null-like fields
    empty_count=$(grep -o ',,' "$file" | wc -l)
    echo "Empty field occurrences: $empty_count" | tee -a "$REPORT"
done

echo ""                                      | tee -a "$REPORT"
echo "Profile complete. Report saved to:"   | tee -a "$REPORT"
echo "$REPORT"                              | tee -a "$REPORT"

# ---- end of script ----

Make it executable and run:
chmod +x scripts/profile_raw_data.sh
./scripts/profile_raw_data.sh

Key concepts in this script:
- #!/bin/bash       → shebang line
- set -euo pipefail → safety settings
- for file in *.csv → loop over files
- $() syntax        → command substitution
- tee               → write to screen AND file
- tee -a            → append to file

---

STEP 2.3 — Deeper Linux inspection

Run these on specific files:

1. Orders file — check unique statuses:
   cut -d',' -f9 raw/olist_orders_dataset.csv |
   sort | uniq -c | sort -rn

2. Count orders per status:
   awk -F',' 'NR>1 {print $9}'
   raw/olist_orders_dataset.csv |
   sort | uniq -c

3. Find rows with empty fields:
   awk -F',' '{for(i=1;i<=NF;i++)
   if($i=="") empty++}
   END{print "Empty cells:", empty}'
   raw/olist_orders_dataset.csv

4. Check payment types:
   cut -d',' -f4
   raw/olist_order_payments_dataset.csv |
   sort | uniq -c | sort -rn

5. Find duplicate order IDs in orders:
   cut -d',' -f1
   raw/olist_orders_dataset.csv |
   sort | uniq -d | wc -l

---

STEP 2.4 — Process management commands

When you run a long PySpark job,
you need to manage the process.

Run a background job:
nohup python3 src/ingestion/profiler.py
> logs/profiler.log 2>&1 &

# nohup → continues after terminal closes
# > logs/profiler.log → redirect stdout
# 2>&1 → also redirect stderr to same file
# & → run in background

Check what is running:
ps aux | grep python
ps aux | grep pyspark

Get the process ID of a specific job:
pgrep -f "profiler.py"

Kill a stuck job:
kill -9 <PID>
pkill -f "profiler.py"

Check if a port is in use (Spark UI):
ss -tlnp | grep 4040

---

STEP 2.5 — Git commit Phase 2

git add scripts/ logs/
git commit -m "Phase 2: Linux profiling
  shell script, raw data inspection."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 3 — PYTHON INGESTION LAYER
Topics: Pandas, exception handling,
        logging, JSON, file handling,
        data validation, modular code
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Create: src/ingestion/profiler.py

This script profiles all 8 CSV files,
validates them, logs every step,
and saves a JSON summary report.

import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv("config.env")

from src.utils.logger import get_logger
logger = get_logger("profiler")

RAW_PATH     = os.getenv("RAW_PATH", "./raw")
REPORTS_PATH = os.getenv("REPORTS_PATH",
                          "./reports")

# ─────────────────────────────────────────
# FUNCTION 1: safe_read
# ─────────────────────────────────────────
def safe_read(filepath: str,
              **kwargs) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(filepath, **kwargs)
        logger.info(f"Loaded: {filepath} "
                    f"| {df.shape[0]} rows "
                    f"| {df.shape[1]} cols")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"Empty file: {filepath}")
        return None
    except pd.errors.ParserError as e:
        logger.error(f"Parse error in {filepath}"
                     f": {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None

# ─────────────────────────────────────────
# FUNCTION 2: profile_dataframe
# ─────────────────────────────────────────
def profile_dataframe(df: pd.DataFrame,
                      name: str) -> dict:
    null_counts = df.isnull().sum().to_dict()
    null_pct = (
        (df.isnull().sum() / len(df) * 100)
        .round(2)
        .to_dict()
    )
    dup_count = df.duplicated().sum()

    profile = {
        "table":          name,
        "rows":           int(len(df)),
        "columns":        int(df.shape[1]),
        "column_names":   list(df.columns),
        "dtypes":         {c: str(t)
                           for c, t in
                           df.dtypes.items()},
        "null_counts":    null_counts,
        "null_pct":       null_pct,
        "duplicate_rows": int(dup_count),
        "memory_mb":      round(
            df.memory_usage(deep=True)
            .sum() / 1024**2, 2)
    }

    logger.info(
        f"{name}: {len(df)} rows | "
        f"{dup_count} duplicates | "
        f"Nulls: {sum(null_counts.values())}"
    )
    return profile

# ─────────────────────────────────────────
# FUNCTION 3: validate_schema
# ─────────────────────────────────────────
EXPECTED_SCHEMAS = {
    "orders": [
        "order_id", "customer_id",
        "order_status", "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "order_items": [
        "order_id", "order_item_id",
        "product_id", "seller_id",
        "shipping_limit_date",
        "price", "freight_value"
    ],
    "customers": [
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city", "customer_state"
    ],
    "products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ],
    "payments": [
        "order_id", "payment_sequential",
        "payment_type", "payment_installments",
        "payment_value"
    ],
    "reviews": [
        "review_id", "order_id",
        "review_score", "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp"
    ]
}

def validate_schema(df: pd.DataFrame,
                    name: str,
                    expected_cols: list) -> bool:
    actual = set(df.columns)
    expected = set(expected_cols)
    missing = expected - actual
    extra   = actual - expected

    if missing:
        logger.warning(
            f"{name}: Missing columns: {missing}")
    if extra:
        logger.info(
            f"{name}: Extra columns: {extra}")

    is_valid = len(missing) == 0
    logger.info(
        f"{name} schema valid: {is_valid}")
    return is_valid

# ─────────────────────────────────────────
# MAIN: run profiler on all files
# ─────────────────────────────────────────
def main():
    logger.info("="*50)
    logger.info("OLIST DATA PROFILER STARTED")
    logger.info("="*50)

    files = {
        "orders":
            "olist_orders_dataset.csv",
        "order_items":
            "olist_order_items_dataset.csv",
        "customers":
            "olist_customers_dataset.csv",
        "products":
            "olist_products_dataset.csv",
        "sellers":
            "olist_sellers_dataset.csv",
        "payments":
            "olist_order_payments_dataset.csv",
        "reviews":
            "olist_order_reviews_dataset.csv",
        "category_translation":
            "product_category_name_translation.csv"
    }

    all_profiles = {}

    for name, filename in files.items():
        path = os.path.join(RAW_PATH, filename)
        df = safe_read(path, low_memory=False)

        if df is None:
            logger.error(
                f"Skipping {name} — load failed")
            continue

        # Profile
        profile = profile_dataframe(df, name)
        all_profiles[name] = profile

        # Validate schema
        if name in EXPECTED_SCHEMAS:
            validate_schema(
                df, name,
                EXPECTED_SCHEMAS[name])

    # Save JSON report
    os.makedirs(REPORTS_PATH, exist_ok=True)
    report_path = os.path.join(
        REPORTS_PATH,
        f"raw_profile_{datetime.now()
        .strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(report_path, "w") as f:
        json.dump(all_profiles, f,
                  indent=2, default=str)

    logger.info(
        f"Profile report saved: {report_path}")
    logger.info("PROFILER COMPLETE")

if __name__ == "__main__":
    main()

Run it:
python3 src/ingestion/profiler.py

Check logs:
cat logs/profiler_*.log

Check JSON report:
cat reports/raw_profile_*.json | python3
-m json.tool | head -80

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 4 — PYSPARK TRANSFORMATION LAYER
Topics: All covered PySpark +
        cache(), persist(), repartition(),
        coalesce(), broadcast join,
        performance optimization,
        lazy evaluation deep dive
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Create: src/transform/spark_pipeline.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os

load_dotenv("config.env")
from src.utils.logger import get_logger
logger = get_logger("spark_pipeline")

# ─────────────────────────────────────────
# CONCEPT: LAZY EVALUATION — understand first
# ─────────────────────────────────────────
# PySpark does NOT execute transformations
# immediately. It builds a logical plan.
# Only ACTIONS trigger actual execution.
#
# Transformations (lazy — no execution):
#   read, filter, select, withColumn,
#   groupBy, join, withColumnRenamed
#
# Actions (triggers execution):
#   show(), count(), collect(),
#   write(), first(), take(n)
#
# This is WHY PySpark is fast — it
# optimises the full plan before running.
# ─────────────────────────────────────────

def create_spark():
    return SparkSession.builder \
        .appName(os.getenv(
            "SPARK_APP_NAME","OlistPipeline")) \
        .master(os.getenv(
            "SPARK_MASTER","local[*]")) \
        .config("spark.sql.adaptive"
                ".enabled", "true") \
        .config("spark.sql.shuffle"
                ".partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

# ─────────────────────────────────────────
# STEP 4.1 — Read all files
# ─────────────────────────────────────────
def read_all_tables(spark, raw_path):
    logger.info("Reading all raw CSV files")

    read_opts = {
        "header": "true",
        "inferSchema": "true",
        "nullValue": "",
        "emptyValue": None
    }

    orders = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_orders_dataset.csv")

    items = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_order_items"
             "_dataset.csv")

    customers = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_customers"
             "_dataset.csv")

    products = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_products"
             "_dataset.csv")

    sellers = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_sellers"
             "_dataset.csv")

    payments = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_order_payments"
             "_dataset.csv")

    reviews = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_order_reviews"
             "_dataset.csv")

    category_trans = spark.read \
        .options(**read_opts) \
        .csv(f"{raw_path}/product_category"
             "_name_translation.csv")

    logger.info(
        f"Orders: {orders.count()} rows")
    logger.info(
        f"Items:  {items.count()} rows")
    logger.info(
        f"Customers: {customers.count()} rows")

    return (orders, items, customers,
            products, sellers, payments,
            reviews, category_trans)

# ─────────────────────────────────────────
# STEP 4.2 — Clean orders
# ─────────────────────────────────────────
def clean_orders(orders_df):
    logger.info("Cleaning orders table")

    # Convert all date columns
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    df = orders_df
    for col_name in date_cols:
        df = df.withColumn(
            col_name,
            to_timestamp(col(col_name))
        )

    # Add derived columns
    df = df.withColumn(
        "purchase_year",
        year("order_purchase_timestamp")
    ).withColumn(
        "purchase_month",
        month("order_purchase_timestamp")
    ).withColumn(
        "purchase_day_of_week",
        dayofweek("order_purchase_timestamp")
    ).withColumn(
        "delivery_days",
        datediff(
            "order_delivered_customer_date",
            "order_purchase_timestamp"
        )
    ).withColumn(
        "is_late",
        when(
            col("order_delivered_customer_date")
            > col("order_estimated_delivery_date"),
            True
        ).otherwise(False)
    )

    # Drop duplicates on order_id
    before = df.count()
    df = df.dropDuplicates(["order_id"])
    after = df.count()
    logger.info(
        f"Orders: removed {before-after} dupes")

    return df

# ─────────────────────────────────────────
# STEP 4.3 — Clean products + translate
# ─────────────────────────────────────────
def clean_products(products_df,
                   category_trans_df):
    logger.info("Cleaning products table")

    # Fill NULL category names
    df = products_df.fillna(
        {"product_category_name": "unknown"}
    )

    # Fill NULL dimensions with median-like
    # approximation (0 for simplicity)
    df = df.fillna({
        "product_weight_g":       0.0,
        "product_length_cm":      0.0,
        "product_height_cm":      0.0,
        "product_width_cm":       0.0,
        "product_photos_qty":     0,
        "product_name_lenght":    0,
        "product_description_lenght": 0
    })

    # Join with category translation
    # Category translation is a SMALL table
    # This is where BROADCAST JOIN is used
    # Broadcast = copy small table to all
    # executors to avoid shuffle

    from pyspark.sql.functions import broadcast

    df = df.join(
        broadcast(category_trans_df),
        on="product_category_name",
        how="left"
    )

    # Fill untranslated categories
    df = df.withColumn(
        "product_category_name_english",
        coalesce(
            col("product_category_name_english"),
            initcap(
                regexp_replace(
                    col("product_category_name"),
                    "_", " "
                )
            )
        )
    )

    return df

# ─────────────────────────────────────────
# STEP 4.4 — Clean payments
# ─────────────────────────────────────────
def clean_payments(payments_df):
    logger.info("Cleaning payments table")

    df = payments_df \
        .filter(col("payment_value") > 0) \
        .dropna(subset=["order_id",
                        "payment_type"])

    # Aggregate payments per order
    # (one order can have multiple payments)
    payments_agg = df.groupBy("order_id") \
        .agg(
            sum("payment_value")
                .alias("total_payment"),
            count("payment_sequential")
                .alias("payment_count"),
            collect_set("payment_type")
                .alias("payment_types"),
            max("payment_installments")
                .alias("max_installments")
        )

    return payments_agg

# ─────────────────────────────────────────
# STEP 4.5 — Build master order table
#             with all joins
# ─────────────────────────────────────────
def build_master_table(orders_df, items_df,
                       customers_df,
                       products_df,
                       sellers_df,
                       payments_df):
    logger.info("Building master order table")

    # Aggregate order items per order
    items_agg = items_df.groupBy("order_id") \
        .agg(
            count("order_item_id")
                .alias("item_count"),
            sum("price")
                .alias("items_total"),
            sum("freight_value")
                .alias("freight_total"),
            round(
                sum("price") +
                sum("freight_value"), 2
            ).alias("order_total"),
            collect_set("product_id")
                .alias("product_ids"),
            collect_set("seller_id")
                .alias("seller_ids")
        )

    # Chain all joins
    master = orders_df \
        .join(items_agg, "order_id", "left") \
        .join(customers_df, "customer_id", "left") \
        .join(payments_df, "order_id", "left")

    logger.info(
        f"Master table rows: {master.count()}")

    return master

# ─────────────────────────────────────────
# STEP 4.6 — CACHE and PERSIST
#             (new concept today)
# ─────────────────────────────────────────
# After building the master table it will
# be queried many times for different reports.
# Without caching, Spark re-reads and
# re-computes the entire chain every time.
# Cache stores the result in memory.
#
# cache()   = persist in MEMORY only
# persist() = you choose the storage level:
#   MEMORY_ONLY         → fastest, RAM only
#   MEMORY_AND_DISK     → RAM, spills to disk
#   DISK_ONLY           → disk only, slower
#   MEMORY_ONLY_SER     → serialized, less RAM
#
# When to use:
#   Use cache() for DataFrames you will
#   query more than once in the same job.
#   Do NOT cache everything — wastes RAM.
# ─────────────────────────────────────────

from pyspark.storagelevel import StorageLevel

def cache_master_table(master_df):
    logger.info("Caching master table")

    # cache() = MEMORY_AND_DISK by default
    master_df.cache()

    # OR use explicit persist:
    master_df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    # Trigger the cache (force materialisation)
    count = master_df.count()
    logger.info(
        f"Master table cached: {count} rows")

    return master_df

# ─────────────────────────────────────────
# STEP 4.7 — REPARTITION and COALESCE
#             (new concept today)
# ─────────────────────────────────────────
# PySpark distributes data in PARTITIONS.
# By default Spark may create too many or
# too few partitions which slows output.
#
# repartition(n) → shuffle, create exactly
#   n partitions. Even distribution.
#   Use BEFORE heavy joins or aggregations.
#   MORE expensive (full shuffle).
#
# coalesce(n) → reduce partitions without
#   full shuffle. LESS expensive.
#   Use BEFORE writing output files.
#   Use when reducing, not increasing.
#
# Small Files Problem:
#   If you write 200 partitions = 200 files.
#   This kills BigQuery/GCS performance.
#   Always coalesce(1) for small outputs.
#   For large outputs: coalesce(4 or 8).
# ─────────────────────────────────────────

def write_reports(master_df,
                  processed_path):
    logger.info("Writing processed output")

    # Check current partition count
    n_parts = master_df.rdd.getNumPartitions()
    logger.info(
        f"Current partitions: {n_parts}")

    # Repartition for better distribution
    # Use when writing large files
    master_repartitioned = master_df \
        .repartition(4, "customer_state")
    # Repartition by state ensures related
    # data is in the same partition

    # Coalesce for small summary reports
    # (avoids 200 tiny part files)
    master_coalesced = master_df.coalesce(1)

    # Write full dataset partitioned by state
    master_repartitioned.write \
        .mode("overwrite") \
        .partitionBy("customer_state") \
        .parquet(f"{processed_path}/master_orders")

    logger.info(
        "Master orders written as parquet")

    # Write summary as single CSV
    master_coalesced \
        .select(
            "order_id",
            "order_status",
            "purchase_year",
            "purchase_month",
            "delivery_days",
            "is_late",
            "item_count",
            "order_total",
            "total_payment",
            "customer_state"
        ) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"{processed_path}/orders_summary")

    logger.info("Output files written")

# ─────────────────────────────────────────
# STEP 4.8 — PySpark Performance Exercise
#             Using NYC Taxi Parquet
# ─────────────────────────────────────────

def nyc_taxi_performance_exercise(
        spark, raw_path):
    logger.info("Loading NYC Taxi dataset")

    # Read the 3M row parquet file
    taxi = spark.read.parquet(
        f"{raw_path}/nyc_taxi_jan2023.parquet"
    )

    taxi.printSchema()
    logger.info(
        f"Taxi rows: {taxi.count()}")

    # Check partition count
    logger.info(
        f"Partitions: "
        f"{taxi.rdd.getNumPartitions()}")

    # Repartition for parallel processing
    taxi = taxi.repartition(8)

    # Cache it — we will query it 3 times
    taxi.cache()
    taxi.count()  # trigger cache
    logger.info("Taxi data cached")

    # Query 1: Revenue by hour
    import time
    t1 = time.time()
    hourly_rev = taxi.groupBy(
        hour("tpep_pickup_datetime")
        .alias("hour")
    ).agg(
        count("*").alias("trips"),
        round(sum("total_amount"),2)
            .alias("revenue"),
        round(avg("trip_distance"),2)
            .alias("avg_distance")
    ).orderBy("hour")

    hourly_rev.show(24)
    t2 = time.time()
    logger.info(
        f"Query 1 time: {t2-t1:.2f}s")

    # Query 2: Top pickup locations
    t1 = time.time()
    top_zones = taxi.groupBy("PULocationID") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(10)
    top_zones.show()
    t2 = time.time()
    logger.info(
        f"Query 2 time: {t2-t1:.2f}s")

    # Query 3: Payment type breakdown
    t1 = time.time()
    payment_breakdown = taxi.groupBy(
        "payment_type"
    ).agg(
        count("*").alias("trips"),
        round(sum("total_amount"),2)
            .alias("revenue")
    ).orderBy(col("revenue").desc())
    payment_breakdown.show()
    t2 = time.time()
    logger.info(
        f"Query 3 time: {t2-t1:.2f}s")

    # Without cache, each query would
    # re-read and re-parse the parquet file.
    # With cache, queries 2 and 3 are
    # served from memory — much faster.

    # Unpersist when done to free RAM
    taxi.unpersist()
    logger.info("Taxi data unpersisted")

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    raw_path       = os.getenv("RAW_PATH",
                               "./raw")
    processed_path = os.getenv(
        "PROCESSED_PATH", "./processed")

    spark = create_spark()
    logger.info("Spark session created")
    logger.info(
        f"Spark UI: http://localhost:4040")

    # Read
    (orders, items, customers, products,
     sellers, payments, reviews,
     category_trans) = read_all_tables(
         spark, raw_path)

    # Transform
    orders_clean    = clean_orders(orders)
    products_clean  = clean_products(
        products, category_trans)
    payments_clean  = clean_payments(payments)

    # Build master
    master = build_master_table(
        orders_clean, items, customers,
        products_clean, sellers,
        payments_clean
    )

    # Cache for multiple report queries
    master = cache_master_table(master)

    # Register for SparkSQL
    master.createOrReplaceTempView(
        "master_orders")
    orders_clean.createOrReplaceTempView(
        "orders")
    products_clean.createOrReplaceTempView(
        "products")

    # Write output
    write_reports(master, processed_path)

    # Performance exercise
    nyc_taxi_performance_exercise(
        spark, raw_path)

    spark.stop()
    logger.info("Pipeline complete")

if __name__ == "__main__":
    main()

Run:
nohup python3 src/transform/spark_pipeline.py
> logs/spark_pipeline.log 2>&1 &

Monitor:
tail -f logs/spark_pipeline.log
Spark UI: http://localhost:4040

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 5 — SQL ANALYTICS LAYER
Topics: All SQL Level 1+2+3 combined
        CTEs, Window Functions, Views,
        Subqueries, JOINs, CASE — full
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

File: sql/analytics.sql
Platform: freesql.com (Oracle) OR
          SparkSQL via master_orders view

---------------------------------------------
REPORT 1 — ORDER STATUS DASHBOARD
---------------------------------------------

WITH order_stats AS (
    SELECT
        order_status,
        COUNT(*)                    AS total,
        ROUND(AVG(delivery_days),1) AS avg_days,
        SUM(CASE WHEN is_late = 1
            THEN 1 ELSE 0 END)      AS late_count,
        ROUND(
            SUM(CASE WHEN is_late=1
                THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*), 2
        )                           AS late_pct
    FROM orders
    GROUP BY order_status
),
ranked_status AS (
    SELECT *,
        RANK() OVER(ORDER BY total DESC)
            AS popularity_rank
    FROM order_stats
)
SELECT *
FROM   ranked_status
ORDER BY popularity_rank;

---------------------------------------------
REPORT 2 — MONTHLY REVENUE TREND
           WITH LAG COMPARISON
---------------------------------------------

WITH monthly_rev AS (
    SELECT
        purchase_year   AS yr,
        purchase_month  AS mth,
        COUNT(*)        AS orders,
        ROUND(SUM(order_total),2) AS revenue
    FROM orders
    WHERE order_status = 'delivered'
      AND order_total IS NOT NULL
    GROUP BY purchase_year, purchase_month
),
monthly_with_lag AS (
    SELECT
        yr, mth, orders, revenue,
        LAG(revenue) OVER(
            ORDER BY yr, mth
        )                  AS prev_month_rev,
        ROUND(revenue - LAG(revenue) OVER(
            ORDER BY yr, mth
        ), 2)              AS revenue_change,
        ROUND(
            (revenue - LAG(revenue) OVER(
                ORDER BY yr, mth))
            * 100.0
            / NULLIF(LAG(revenue) OVER(
                ORDER BY yr, mth), 0)
        , 2)               AS growth_pct
    FROM monthly_rev
)
SELECT * FROM monthly_with_lag
ORDER BY yr, mth;

---------------------------------------------
REPORT 3 — CUSTOMER STATE PERFORMANCE
           RANKED BY REVENUE
---------------------------------------------

WITH state_stats AS (
    SELECT
        customer_state,
        COUNT(DISTINCT order_id) AS orders,
        COUNT(DISTINCT customer_id)
            AS customers,
        ROUND(SUM(order_total),2)
            AS total_revenue,
        ROUND(AVG(order_total),2)
            AS avg_order_value,
        ROUND(AVG(delivery_days),1)
            AS avg_delivery_days
    FROM orders
    WHERE order_status = 'delivered'
    GROUP BY customer_state
),
state_ranked AS (
    SELECT *,
        DENSE_RANK() OVER(
            ORDER BY total_revenue DESC
        ) AS revenue_rank,
        NTILE(4) OVER(
            ORDER BY total_revenue DESC
        ) AS revenue_quartile
    FROM state_stats
),
final_state AS (
    SELECT *,
        CASE revenue_quartile
            WHEN 1 THEN 'Top Performer'
            WHEN 2 THEN 'Strong'
            WHEN 3 THEN 'Average'
            WHEN 4 THEN 'Below Average'
        END AS performance_tier,
        SUM(total_revenue) OVER()
            AS grand_total,
        ROUND(
            total_revenue * 100.0
            / SUM(total_revenue) OVER()
        , 2) AS revenue_share_pct
    FROM state_ranked
)
SELECT * FROM final_state
ORDER BY revenue_rank;

---------------------------------------------
REPORT 4 — RUNNING TOTAL AND CUMULATIVE
           REVENUE BY DATE
---------------------------------------------

WITH daily_rev AS (
    SELECT
        TRUNC(order_purchase_timestamp)
            AS order_date,
        COUNT(*) AS daily_orders,
        ROUND(SUM(order_total),2)
            AS daily_revenue
    FROM orders
    WHERE order_status = 'delivered'
      AND order_total IS NOT NULL
    GROUP BY TRUNC(order_purchase_timestamp)
)
SELECT
    order_date,
    daily_orders,
    daily_revenue,
    SUM(daily_revenue) OVER(
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING
                 AND CURRENT ROW
    )                        AS cumulative_revenue,
    ROUND(daily_revenue * 100.0
        / SUM(daily_revenue) OVER()
    , 3)                     AS pct_of_total,
    AVG(daily_revenue) OVER(
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING
                 AND CURRENT ROW
    )                        AS rolling_7day_avg
FROM daily_rev
ORDER BY order_date;

---------------------------------------------
REPORT 5 — LATE DELIVERY ANALYSIS
           WITH WINDOW + CASE
---------------------------------------------

SELECT
    customer_state,
    order_status,
    delivery_days,
    CASE
        WHEN delivery_days <= 5  THEN 'Fast'
        WHEN delivery_days <= 15 THEN 'Normal'
        WHEN delivery_days <= 30 THEN 'Slow'
        WHEN delivery_days > 30  THEN 'Very Slow'
        ELSE 'Unknown'
    END AS delivery_speed,
    RANK() OVER(
        PARTITION BY customer_state
        ORDER BY delivery_days DESC
    ) AS slowest_in_state,
    ROUND(AVG(delivery_days) OVER(
        PARTITION BY customer_state
    ), 1) AS state_avg_days,
    delivery_days - ROUND(
        AVG(delivery_days) OVER(
            PARTITION BY customer_state
        ), 1) AS vs_state_avg
FROM orders
WHERE order_status = 'delivered'
  AND delivery_days IS NOT NULL
ORDER BY customer_state, delivery_days DESC;

Create this as a view for reuse:

CREATE OR REPLACE VIEW delivery_analysis_vw AS
SELECT ... (paste full query above) ...;

SELECT * FROM delivery_analysis_vw
WHERE delivery_speed = 'Very Slow'
ORDER BY delivery_days DESC
FETCH FIRST 20 ROWS ONLY;

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 6 — SHELL SCRIPTING AND AUTOMATION
Topics: Shell scripts, cron scheduling,
        environment variables in scripts,
        nohup, process management
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This is the Airflow introduction —
before Airflow you must understand
how pipelines were orchestrated using
shell scripts and cron jobs.

---

STEP 6.1 — Master pipeline runner script

Create: scripts/run_pipeline.sh

#!/bin/bash
# ============================================
# run_pipeline.sh
# Purpose: Orchestrate the full Olist pipeline
# Usage  : ./scripts/run_pipeline.sh [env]
# Example: ./scripts/run_pipeline.sh dev
# ============================================

set -euo pipefail

ENV=${1:-dev}
CONFIG_FILE="config.env"
LOG_DIR="./logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PIPELINE_LOG="$LOG_DIR/pipeline_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" |
    tee -a "$PIPELINE_LOG"
}

check_success() {
    if [ $? -ne 0 ]; then
        log "ERROR: $1 failed. Aborting."
        exit 1
    fi
    log "SUCCESS: $1 completed."
}

# ─────────────────────────────────────────
log "============================================"
log "OLIST PIPELINE STARTED | ENV: $ENV"
log "============================================"

# Load environment
source "$CONFIG_FILE"
log "Config loaded from $CONFIG_FILE"
log "Project: $PROJECT_NAME"
log "Raw path: $RAW_PATH"

# Activate venv
source venv/bin/activate
log "Virtual environment activated"

# Phase 1: Profile raw data
log "PHASE 1: Raw data profiling"
python3 src/ingestion/profiler.py
check_success "Data profiling"

# Phase 2: PySpark transformation
log "PHASE 2: PySpark transformation"
python3 src/transform/spark_pipeline.py
check_success "Spark transformation"

# Phase 3: Copy reports to archive
log "PHASE 3: Archiving outputs"
ARCHIVE_DIR="./archive/run_$TIMESTAMP"
mkdir -p "$ARCHIVE_DIR"
cp -r reports/ "$ARCHIVE_DIR/"
cp -r logs/ "$ARCHIVE_DIR/"
check_success "Archiving"

# Phase 4: Compress archive
log "PHASE 4: Compressing archive"
tar -czf "${ARCHIVE_DIR}.tar.gz" \
    -C archive "run_$TIMESTAMP"
rm -rf "$ARCHIVE_DIR"
check_success "Compression"

log "============================================"
log "PIPELINE COMPLETE"
log "Archive: ${ARCHIVE_DIR}.tar.gz"
log "Log: $PIPELINE_LOG"
log "============================================"

# ---- end of script ----

chmod +x scripts/run_pipeline.sh

---

STEP 6.2 — Functions in shell scripts

Key shell scripting concepts used above:

Variables:
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  VAR=${1:-default}  → use $1, else default

Functions:
  log() { echo "[$(date)] $1"; }
  Call it: log "message here"

Conditions:
  if [ $? -ne 0 ]; then
    echo "failed"
    exit 1
  fi
  $? = exit code of last command
  0 = success, non-zero = failure

source vs ./
  source file.sh → runs in CURRENT shell
                   (variables persist)
  ./file.sh      → runs in NEW shell
                   (variables lost after)

---

STEP 6.3 — Schedule with cron

cron is the Linux job scheduler.
Think of it as a basic Airflow.

Edit your crontab:
crontab -e

Add this line to run the pipeline
every day at 2:00 AM:

0 2 * * * cd /path/to/olist_pipeline &&
./scripts/run_pipeline.sh >>
./logs/cron.log 2>&1

Cron syntax: minute hour day month weekday
  0 2 * * * → minute=0, hour=2,
               every day, every month,
               every weekday

Other examples:
*/15 * * * * → every 15 minutes
0 */6 * * * → every 6 hours
0 9 * * 1   → every Monday at 9 AM
0 0 1 * *   → first of every month at midnight

View current cron jobs:
crontab -l

Remove all cron jobs:
crontab -r

---

STEP 6.4 — Git commit Phase 6

git add scripts/
git commit -m "Phase 6: Shell scripts,
  pipeline runner, cron scheduling."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 7 — AIRFLOW INTRODUCTION
Topic: Apache Airflow — concepts and
       first DAG (Job Ready pending)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Airflow is an orchestration tool.
It does what run_pipeline.sh does but:
- Has a web UI to monitor
- Handles retries automatically
- Shows task dependencies visually
- Used by every data engineering team

Core concepts:
DAG      → Directed Acyclic Graph.
           A Python file describing
           the pipeline and its tasks.
Task     → One step in the pipeline.
Operator → Type of task:
           PythonOperator → run Python
           BashOperator   → run shell cmd
           SparkSubmitOperator → run Spark
Trigger  → schedule_interval like cron

Install Airflow locally (first time):
pip install apache-airflow

Initialize the database:
airflow db init

Start the web UI:
airflow webserver -p 8080

# Activate the virtual environment
source ~/airflow-env/bin/activate

# Start Airflow standalone
airflow standalone

Open in browser:
http://localhost:8080

---

STEP 7.1 — Write your first DAG

Create: dags/olist_pipeline_dag.py

from airflow import DAG
from airflow.operators.python import (
    PythonOperator
)
from airflow.operators.bash import (
    BashOperator
)
from datetime import datetime, timedelta
import sys
sys.path.append("/path/to/olist_pipeline")

from src.ingestion.profiler import main \
    as run_profiler

# Default arguments applied to all tasks
default_args = {
    "owner":           "kode-x",
    "depends_on_past": False,
    "start_date":      datetime(2024, 1, 1),
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
}

# Define the DAG
with DAG(
    dag_id="olist_pipeline",
    default_args=default_args,
    description="Olist e-commerce pipeline",
    ="0 2 * * *",
    catchschedule_intervalup=False,
    tags=["olist", "etl", "production"]
) as dag:

    # Task 1: Profile raw data
    task_profile = PythonOperator(
        task_id="profile_raw_data",
        python_callable=run_profiler
    )

    # Task 2: Run PySpark transformation
    task_spark = BashOperator(
        task_id="spark_transformation",
        bash_command=(
            "cd /path/to/olist_pipeline && "
            "source venv/bin/activate && "
            "python3 src/transform/"
            "spark_pipeline.py"
        )
    )

    # Task 3: Archive outputs
    task_archive = BashOperator(
        task_id="archive_outputs",
        bash_command=(
            "tar -czf "
            "/path/to/archive/olist_"
            "$(date +%Y%m%d).tar.gz "
            "/path/to/olist_pipeline/reports/"
        )
    )

    # Define task dependencies
    # This is the pipeline flow:
    # profile → spark → archive
    task_profile >> task_spark >> task_archive

Key concepts in this DAG:
>> operator → defines task order
             (must run before)
schedule_interval → like cron expression
catchup=False     → do not backfill
retries=2         → retry twice on failure
tags              → searchable labels in UI

---

STEP 7.2 — Trigger and monitor

List all DAGs:
airflow dags list

Trigger manually (without waiting for cron):
airflow dags trigger olist_pipeline

Check task status:
airflow tasks list olist_pipeline

Check logs of a specific task:
airflow tasks logs olist_pipeline
    profile_raw_data 2024-01-01

In the web UI at http://localhost:8080:
- Green   = success
- Red     = failed
- Yellow  = running
- Grey    = not yet run

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 8 — GIT FINAL WORKFLOW
Topic: Git complete workflow
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Professional Git workflow used in
every data engineering team:

# Check what changed
git status
git diff

# Stage specific files
git add src/transform/spark_pipeline.py
git add sql/analytics.sql

# Stage all changes
git add .

# Commit with meaningful message
git commit -m "feat: add PySpark
  transformation with cache and persist.
  Covers Olist orders, payments, products."

# View history
git log --oneline

# Create a feature branch (standard practice)
git checkout -b feature/spark-optimisation

# Switch back to main
git checkout main

# Merge the feature branch
git merge feature/spark-optimisation

# If you have GitHub:
git remote add origin
    https://github.com/your-username/
    olist-pipeline.git
git push -u origin main

Final commit for the whole project:
git add .
git commit -m "feat: Day 14 capstone
  complete. Olist pipeline — ingest,
  transform, analyse, orchestrate.
  PySpark cache/persist/repartition,
  window functions, CTEs, shell scripting,
  cron, Airflow DAG, Git workflow."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DELIVERABLES CHECKLIST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Phase 1 Setup
  Git repo initialised
  .gitignore created
  venv created, requirements.txt installed
  config.env created and loaded
  Logger utility written

Phase 2 Linux
  profile_raw_data.sh runs cleanly
  All 8 CSV files profiled via shell
  Process management commands tested

Phase 3 Python
  profiler.py runs without errors
  JSON profile report saved
  All 8 files validated against schema
  Logger writes to file and console

Phase 4 PySpark
  All 8 tables loaded
  Orders cleaned: dates, delivery_days,
  is_late derived
  Products joined with category translation
  via broadcast join
  Payments aggregated per order
  Master table built with all joins
  Master table cached
  Output written as parquet (partitioned)
  and CSV (coalesced to 1)
  NYC Taxi exercise: cache timing compared

Phase 5 SQL
  All 5 SQL reports written and run
  Delivery analysis view created
  Window functions used in all 5 queries
  CTEs used in all 5 queries

Phase 6 Shell
  run_pipeline.sh script complete
  Runs end-to-end without errors
  Cron job configured
  Archive created on each run

Phase 7 Airflow
  First DAG written
  Airflow UI accessible
  DAG triggered manually
  3 tasks visible in UI

Phase 8 Git
  Minimum 5 commits across phases
  Feature branch used at least once
  Clean git log --oneline output

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SYLLABUS TOPICS COVERED IN THIS PROJECT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Python:
  Modules 1–6 fully applied
  Logging (Module 4)
  Exception handling (Module 4)
  JSON report generation (Module 4)
  os.getenv, dotenv (Module 6)
  Pandas profiling (Module 6)
  Functions, *args, **kwargs (Module 3)

PySpark:
  All covered operations revised
  cache() and persist() — NEW
  repartition() and coalesce() — NEW
  Broadcast join — deeper
  StorageLevel — NEW
  Lazy evaluation explained — NEW
  Small files problem — NEW
  Spark performance timing — NEW
  SparkSQL across all queries

SQL:
  All Level 1 + 2 + 3 applied
  CTEs (WITH clause)
  Window: ROW_NUMBER, RANK, DENSE_RANK,
          NTILE, LAG, FIRST_VALUE
  Running total ROWS BETWEEN
  Rolling average
  Views (CREATE OR REPLACE VIEW)
  NULLIF for divide by zero
  COALESCE for NULL handling

Linux:
  Shell scripting — REVISION
  Functions in shell — NEW
  Cron scheduling — NEW
  nohup background jobs — NEW
  Process management — NEW
  set -euo pipefail — NEW
  tee for logging — revision

Job Ready:
  Git full workflow — NEW
  .gitignore — NEW
  requirements.txt — NEW
  venv — NEW
  config.env + dotenv — NEW
  Logging module — NEW
  Apache Airflow DAG — NEW
  BashOperator, PythonOperator — NEW
  Task dependencies with >> — NEW
  Project directory structure — NEW

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXPECTED COMPLETION TIME
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Phase 1 Setup      : 30 minutes
Phase 2 Linux      : 45 minutes
Phase 3 Python     : 60 minutes
Phase 4 PySpark    : 90 minutes
Phase 5 SQL        : 60 minutes
Phase 6 Shell      : 45 minutes
Phase 7 Airflow    : 45 minutes
Phase 8 Git        : 30 minutes

Total estimated    : 6 to 7 hours
Spread over        : 2 to 3 days is fine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━