import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
)
from src.transforms.spark_utils import get_spark
from src.transforms.clean_sales import dedup_order_items, handle_nulls
from src.transforms.clean_reviews import clean_review_text, compute_word_count


@pytest.fixture(scope="module")
def spark():
    return get_spark()


def test_dedup_order_items(spark):
    # two rows share the same (order_id, order_item_id) -- should collapse to one
    df = spark.createDataFrame(
        [("O1", 1, 10.0), ("O1", 1, 10.0), ("O1", 2, 20.0)],
        ["order_id", "order_item_id", "price"],
    )
    assert dedup_order_items(df).count() == 2


def test_handle_nulls_fills_price(spark):
    # price/freight_value are entirely null here, so Spark can't infer their
    # types -- pass an explicit schema instead of relying on inference.
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("order_item_id", LongType()),
        StructField("price", DoubleType()),
        StructField("freight_value", DoubleType()),
    ])
    df = spark.createDataFrame([("O1", 1, None, None)], schema)
    result = handle_nulls(df)
    row = result.collect()[0]
    assert row["price"] == 0 and row["freight_value"] == 0


def test_clean_review_text_replaces_null(spark):
    # review_comment_message is entirely null -- explicit schema needed.
    schema = StructType([
        StructField("review_id", StringType()),
        StructField("review_comment_message", StringType()),
    ])
    df = spark.createDataFrame([("R1", None)], schema)
    result = clean_review_text(df)
    assert result.collect()[0]["review_comment_message"] == ""


def test_word_count(spark):
    df = spark.createDataFrame([("R1", "great fast delivery")], ["review_id", "review_comment_message"])
    result = compute_word_count(clean_review_text(df))
    assert result.collect()[0]["review_word_count"] == 3
