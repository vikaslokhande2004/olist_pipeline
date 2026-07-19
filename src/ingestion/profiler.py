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
def safe_read(filepath: str, **kwargs) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(filepath,**kwargs)
        logger.info(f"Loaded: {filepath} "
                    f"| {df.shape[0]} rows "
                    f"| {df.shape[1]} cols")
        return df
    except FileNotFoundError:
        logger.error(f"File Not found: {filepath}")
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"Empty file: {filepath}")
        return None
    except pd.errors.ParserError as e:
        logger.error(f"Parse error in{filepath}"
                     f": {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None

# ─────────────────────────────────────────
# FUNCTION 2: profile_dataframe
# ─────────────────────────────────────────
def profile_dataframe(df: pd.DataFrame,name: str) -> dict:
    null_counts = df.isnull().sum().to_dict()
    null_pct =(
        (df.isnull().sum() / len(df) * 100)
        .round(2)
        .to_dict()
    )
    dup_count = df.duplicated().sum()
    
    profile = {
        "table":          name,
        "rows":           int(len(df)),
        "columns":        int(df.shape[1]),
        "column_names":        list(df.columns),
        "dtypes":         {c: str(t) for c,t in df.dtypes.items()},
        "null_counts":    null_counts,
        "null_pct":       null_pct,
        "duplicate_rows": int(dup_count),
        "memory_mb":      round(df.memory_usage(deep=True).sum() / 1024**2,2)   
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

def validate_schema(df: pd.DataFrame,name: str,expected_cols: list) -> bool:
    actual = set(df.columns)
    expected = set(expected_cols)
    missing = expected - actual
    extra = actual - expected

    if missing:
        logger.warning(f"{name}: Missing columns: {missing}")
    if extra:
        logger.info(f"{name}: Extra columns: {extra}")
    is_valid = len(missing) == 0
    logger.info(f"{name} schema valied: {is_valid}")

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
        path = os.path.join(RAW_PATH,filename)
        df = safe_read(path,low_memory=False)

        if df is None:
            logger.error(f"Skipping {name} - load failed")
            continue
        
        # Profile
        profile = profile_dataframe(df,name)
        all_profiles[name] = profile

        # Validate schema
        if name in EXPECTED_SCHEMAS:
            validate_schema(
                df,name,EXPECTED_SCHEMAS[name]
            )
        
    # Save Json report
    os.makedirs(REPORTS_PATH,exist_ok=True)
    report_path = os.path.join(
        REPORTS_PATH,
        f"raw_profile_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json"
    )
    with open(report_path,"w") as f:
        json.dump(all_profiles,f,indent=2,default=str)
        
    logger.info(f"Profile report saved: {report_path}")
    logger.info("PROFILER COMPLETE")

if __name__ == "__main__":
    main()