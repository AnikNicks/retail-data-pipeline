from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.ingestion.ingest_orders import ingest_orders
from src.ingestion.ingest_order_items import ingest_order_items
from src.ingestion.ingest_reviews import ingest_reviews
from src.ingestion.ingest_dimensions import ingest_dimensions
from src.transforms.clean_sales import clean_sales
from src.transforms.clean_reviews import clean_reviews
from src.transforms.load_gold import (
    load_dim_customer, load_dim_seller, load_dim_product,
    load_dim_date, load_dim_order, load_fact_sales, load_fact_reviews,
    export_gold_parquet,
)
from src.quality_checks.check_gold import run_checks

default_args = {"retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="retail_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",  # Airflow 3.x renamed schedule_interval -> schedule
    catchup=False,
    default_args=default_args,
) as dag:

    t_orders = PythonOperator(task_id="ingest_orders", python_callable=ingest_orders)
    t_items = PythonOperator(task_id="ingest_order_items", python_callable=ingest_order_items)
    t_reviews = PythonOperator(task_id="ingest_reviews", python_callable=ingest_reviews)
    t_dims = PythonOperator(task_id="ingest_dimensions", python_callable=ingest_dimensions)

    t_clean_sales = PythonOperator(task_id="clean_sales", python_callable=clean_sales)
    t_clean_reviews = PythonOperator(task_id="clean_reviews", python_callable=clean_reviews)

    t_load_dims = PythonOperator(
        task_id="load_dims",
        python_callable=lambda: (load_dim_customer(), load_dim_seller(), load_dim_product()),
    )
    t_load_date = PythonOperator(task_id="load_dim_date", python_callable=load_dim_date)
    t_load_order = PythonOperator(task_id="load_dim_order", python_callable=load_dim_order)
    t_load_sales = PythonOperator(task_id="load_fact_sales", python_callable=load_fact_sales)
    t_load_reviews = PythonOperator(task_id="load_fact_reviews", python_callable=load_fact_reviews)

    t_quality = PythonOperator(task_id="quality_checks", python_callable=run_checks)
    t_export = PythonOperator(task_id="export_gold", python_callable=export_gold_parquet)

    # orders and order_items ingest independently, then join in clean_sales
    [t_orders, t_items] >> t_clean_sales
    t_reviews >> t_clean_reviews
    t_dims >> t_load_dims

    # dim_date depends on cleaned sales (dates come from the data itself),
    # and fact_sales needs dim_date + dims loaded first for its FKs
    t_clean_sales >> t_load_date >> t_load_sales
    t_load_dims >> t_load_sales
    t_clean_reviews >> t_load_reviews

    # dim_order (the fact_sales <-> fact_reviews bridge) is built from
    # sales_clean and FK-references dim_customer + dim_date
    [t_clean_sales, t_load_dims, t_load_date] >> t_load_order

    [t_load_sales, t_load_reviews, t_load_order] >> t_quality

    # mirror the validated gold tables to data/gold/*.parquet
    t_quality >> t_export