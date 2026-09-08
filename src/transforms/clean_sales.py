from src.transforms.spark_utils import get_spark, write_overwrite_parquet


def dedup_order_items(df):
    # (order_id, order_item_id) is the real composite key at this grain --
    # a plain order_id dedup would wrongly collapse multi-item orders
    return df.dropDuplicates(["order_id", "order_item_id"])


def dedup_orders(df):
    return df.dropDuplicates(["order_id"])


def handle_nulls(df):
    # a missing price/freight is treated as 0 rather than dropped -- keeps
    # the row (and its product/customer info) instead of losing the order
    return df.na.fill({"price": 0, "freight_value": 0})


def clean_sales():
    # imported here (not at module scope) so Airflow can parse the DAG
    # without pyspark installed in the scheduler/dag-processor image
    from pyspark.sql.functions import col, to_date

    spark = get_spark()

    orders = spark.read.parquet("data/bronze/structured/orders.parquet")
    order_items = spark.read.parquet("data/bronze/structured/order_items.parquet")

    orders = dedup_orders(orders)
    order_items = dedup_order_items(order_items)
    order_items = handle_nulls(order_items)

    # inner join: an order_item with no matching order shouldn't exist in
    # clean data, and we don't want to silently carry orphans into gold
    sales = order_items.join(
        orders.select("order_id", "customer_id", "order_purchase_timestamp"),
        on="order_id",
        how="inner",
    )
    sales = sales.withColumn("date_key", to_date(col("order_purchase_timestamp")))

    write_overwrite_parquet(sales, "data/silver/sales_clean.parquet")
    spark.stop()


if __name__ == "__main__":
    clean_sales()