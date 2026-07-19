from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    "owner": "kode-x",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="olist_pipeline",
    default_args=default_args,
    description="Olist E-Commerce ETL Pipeline",
    schedule="0 2 * * *",      # Every day at 2 AM
    catchup=False,
    tags=["olist", "etl", "pyspark"],
) as dag:

    # ----------------------------------------
    # Task 1 : Profile Raw Data
    # ----------------------------------------
    task_profile = BashOperator(
        task_id="profile_raw_data",
        bash_command="""
        cd /mnt/d/olist_pipeline &&
        source venv_linux/bin/activate &&
        python3 -m src.ingestion.profiler
        """
    )

    # ----------------------------------------
    # Task 2 : Spark Transformation
    # ----------------------------------------
    task_spark = BashOperator(
        task_id="spark_transformation",
        bash_command="""
        cd /mnt/d/olist_pipeline &&
        source venv_linux/bin/activate &&
        python3 -m src.transform.spark_pipeline
        """
    )

    # ----------------------------------------
    # Task 3 : Archive Reports
    # ----------------------------------------
    task_archive = BashOperator(
        task_id="archive_outputs",
        bash_command="""
        cd /mnt/d/olist_pipeline &&
        mkdir -p archive &&
        tar -czf archive/olist_$(date +%Y%m%d).tar.gz reports/
        """
    )

    # Pipeline Order
    task_profile >> task_spark >> task_archive