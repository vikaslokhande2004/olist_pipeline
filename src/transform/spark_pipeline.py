from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions
from dotenv import load_dotenv
import os

load_dotenv("config.env")
from src.utils.logger import get_logger
logger = get_logger("spark_pipeline")


def create_spark():
    return SparkSession.builder \
        .appName(os.getenv(
            "SPARK_APP_NAME","OlistPipeline")) \
        .master(os.getenv(
            "SPARK_MASTER","local[*]")) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

# ─────────────────────────────────────────
# STEP 4.1 — Read all files
# ─────────────────────────────────────────

def read_all_tables(spark,raw_path):
    logger.info("Reading all raw csv files")

    read_opts = {
        "header": "true",
        "inferSchema": "true",
        "nullValue": "",
        "emptyValue": None
    }

    orders = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_orders_dataset.csv")
    
    items = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_order_items_dataset.csv")
    
    customers = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_customers_dataset.csv")
    
    products = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_products_dataset.csv")
    
    sellers = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_sellers_dataset.csv")
    
    payments = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_order_payments_dataset.csv")
    
    reviews = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/olist_order_reviews_dataset.csv")
    
    category_trans = spark.read.options(**read_opts) \
        .csv(f"{raw_path}/product_category_name_translation.csv")
    

    logger.info(f"Orders: {orders.count()} rows")
    logger.info(f"Items: {items.count()} rows")
    logger.info(f"Customers: {customers.count()} rows")

    return(orders,items,customers,products,sellers,payments,reviews,category_trans)


# ─────────────────────────────────────────
# STEP 4.2 — Clean orders
# ─────────────────────────────────────────
#"order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"

def clean_orders(orders_df):
    logger.info("Cleaning orders table")

    # Converts all data columns
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    df = orders_df
    
    for col_name in date_cols:
        df = df.withColumn(col_name, to_timestamp(col(col_name)))
    
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
        when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), True)
        .otherwise(False)
    )

    # Drop duplicates on order_id
    before = df.count()
    df = df.dropDuplicates(["order_id"])
    after = df.count()
    logger.info(f"Orders: removed {before-after} dupes")

    return df


# ─────────────────────────────────────────
# STEP 4.3 — Clean products + translate
# ─────────────────────────────────────────
# products - "product_id","product_category_name","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"

# translate - product_category_name,product_category_name_english

def clean_products(products_df,category_trans_df):
    logger.info("Cleaning products table")

    # Fill Null category naes
    df = products_df.fillna(
        {"product_category_name" : "unknown"}
    )

    # Fill Null dimensions with median-like
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
                    "_"," "
                )
            )
        )
    )

    return df

# ─────────────────────────────────────────
# STEP 4.4 — Clean payments
# ─────────────────────────────────────────
# "order_id","payment_sequential","payment_type","payment_installments","payment_value"
def clean_payments(payments_df):
    logger.info("Cleaning payments table")

    df = payments_df.filter(col("payment_value") > 0) \
        .dropna(subset=["order_id","payment_type"])
    
    # Aggregate payments per order
    # (one order can have multiple payments)
    payments_agg = df.groupBy("order_id") \
        .agg(
            sum("payment_value").alias("total_payment"),
            count("payment_sequential").alias("payment_count"),
            collect_set("payment_type").alias("payment_types"),
            max("payment_installments").alias("max_installments")
        )
    
    return payments_agg


# ─────────────────────────────────────────
# STEP 4.5 — Build master order table
#             with all joins
# ─────────────────────────────────────────
def build_master_table(orders_df, items_df,customers_df,products_df,sellers_df,payments_df):
    logger.info("Building master order table")

    #Aggregate order items per order
    items_agg = items_df.groupBy("order_id") \
        .agg(
            count("order_item_id").alias("items_count"),
            sum("price").alias("items_total"),
            sum("freight_value").alias("freight_total"),
            round(
                sum("price") + sum("freight_value"),2
            ).alias("order_total"),
            collect_set("product_id").alias("product_ids"),
            collect_set("seller_id").alias("seller_ids")
        )
    
    # Chain all joins
    master = orders_df.join(items_agg,"order_id","left").join(customers_df,"customer_id","left").join(payments_df,"order_id","left")
    
    logger.info(f"Master table rows: {master.count()}")

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
    logger.info(f"Master tbale cached: {count} rows")

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
def write_reports(master_df,processed_path):
    logger.info("Writing processed output")

    # Check current partition count
    n_parts = master_df.rdd.getNumPartitions()
    logger.info(f"Current partitions: {n_parts}")

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
    
    logger.info("Master orders written as parquet")

    # Write summary as single CSV
    master_coalesced \
        .select(
            "order_id",
            "order_status",
            "purchase_year",
            "purchase_month",
            "delivery_days",
            "is_late",
            "items_count",
            "order_total",
            "total_payment",
            "customer_state"
        ) \
        .write \
        .mode("overwrite") \
        .option("header",True) \
        .csv(f"{processed_path}/orders_summary")
    
    logger.info("Output files written")


# ─────────────────────────────────────────
# STEP 4.8 — PySpark Performance Exercise
#             Using NYC Taxi Parquet
# ─────────────────────────────────────────

def nyc_taxi_performance_exercise(spark, raw_path):
    logger.info("Loading NYC Taxi dataset")

    # Read the 3M row parquet file
    taxi = spark.read.parquet(f"{raw_path}/yellow_tripdata_2023-01.parquet")

    taxi.printSchema()
    logger.info(f"Taxi rows: {taxi.count()}")

    # Check partition count
    logger.info(f"Partitions: "
                f"{taxi.rdd.getNumPartitions()}")
    
    # Repartition for parallel processing
    taxi = taxi.repartition(8)

    # Cache it - we will query it 3 times
    taxi.cache()
    taxi.count() # Trigger cache
    logger.info("Taxi data cached")

    # Query 1: Revenue by hour
    import time
    t1 = time.time()
    hourly_rev = taxi.groupBy(
        hour("tpep_pickup_datetime").alias("hour")
    ).agg(
        count("*").alias("trips"),
        round(sum("total_amount"),2).alias("revenue"),
        round(avg("trip_distance"),2).alias("avg_distance")
    ).orderBy("hour")

    hourly_rev.show(24)
    t2 = time.time()
    logger.info(f"Query 1 time: {t2-t1:.2f}s")

    # Query 2: Top pickup locations
    t1 = time.time()
    top_zones = taxi.groupBy("PULocationID") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(10)
    top_zones.show()
    t2 = time.time()
    logger.info(f"Query 2 time: {t2-t1:.2f}s")

    # Query 3: Payment type breakdown
    t1 = time.time()
    payment_breakdown = taxi.groupBy(
        "payment_type"
    ).agg(
        count("*").alias("trips"),
        round(sum("total_amount"),2).alias("revenue")
    ).orderBy(col("revenue").desc())
    payment_breakdown.show()
    t2 = time.time()
    logger.info(f"Query 3 time: {t2-t1:.2f}s")

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
    raw_path = os.getenv("RAW_PATH","./raw")
    processed_path = os.getenv("PROCESSED_PATH","./processed")

    spark = create_spark()
    logger.info("Spark session created")
    logger.info(f"Spark UI: http://localhost:4040")

    # Read
    (orders,items,customers,products,sellers,payments,reviews,category_trans) = read_all_tables(spark,raw_path)

    # Transform
    orders_clean = clean_orders(orders)
    products_clean = clean_products(products,category_trans)
    payments_clean = clean_payments(payments)

    # Build master
    master = build_master_table(orders_clean,items,customers,products_clean,sellers,payments_clean)

    # Cache for multiple repoart queries
    master = cache_master_table(master)

    # Register for SparkSQL
    master.createOrReplaceTempView("master_orders")
    orders_clean.createOrReplaceTempView("orders")
    products_clean.createOrReplaceTempView("products")

    # Write output
    write_reports(master,processed_path)

    # Performance exercise
    nyc_taxi_performance_exercise(spark,raw_path)

    spark.stop()

    logger.info("Pipeline complete")


if __name__ == "__main__":
    main()
