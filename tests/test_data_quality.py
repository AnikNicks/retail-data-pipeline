import pandas as pd

# These check the actual data, not the code -- they'd fail even with
# perfect transform logic if the source data itself had a real problem.


def test_schema_has_expected_columns():
    df = pd.read_parquet("data/silver/sales_clean.parquet")
    expected = {"order_id", "order_item_id", "customer_id", "product_id", "price"}
    assert expected.issubset(set(df.columns))


def test_no_duplicate_order_item_keys():
    df = pd.read_parquet("data/silver/sales_clean.parquet")
    assert not df.duplicated(subset=["order_id", "order_item_id"]).any()


def test_reviews_reference_valid_orders():
    orders = pd.read_parquet("data/bronze/structured/orders.parquet")
    reviews = pd.read_parquet("data/silver/reviews_clean.parquet")
    orphans = reviews[~reviews["order_id"].isin(orders["order_id"])]
    assert len(orphans) == 0, f"{len(orphans)} reviews reference missing orders"


def test_reconciliation_revenue_matches_source():
    # classic "does the warehouse match the source" check -- 1% tolerance
    # allows for rows legitimately excluded by watermark timing
    raw_total = pd.read_csv("data/raw/olist/olist_order_items_dataset.csv")["price"].sum()
    silver_total = pd.read_parquet("data/silver/sales_clean.parquet")["price"].sum()
    assert abs(raw_total - silver_total) / raw_total < 0.01