import os

import pandas as pd
import psycopg2
from src.config import DB_CONFIG

# Analytical gold tables (the warehouse IS the gold layer). pipeline_watermarks
# is operational bookkeeping, so it's excluded from the parquet export.
GOLD_TABLES = [
    "dim_customer", "dim_date", "dim_order", "dim_product",
    "dim_seller", "fact_sales", "fact_reviews",
]

# Everything here uses ON CONFLICT ... DO UPDATE (upsert) instead of plain
# INSERT, so re-running the load after a crash/retry never creates
# duplicate rows -- idempotency matters more than speed at this scale.


def load_dim_customer():
    df = pd.read_parquet("data/bronze/structured/customers.parquet")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_customer (customer_id, customer_city, customer_state)
            VALUES (%s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_city = EXCLUDED.customer_city,
                customer_state = EXCLUDED.customer_state
            """,
            (row.customer_id, row.get("customer_city"), row.get("customer_state")),
        )
    conn.commit()
    conn.close()


def load_dim_seller():
    df = pd.read_parquet("data/bronze/structured/sellers.parquet")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_seller (seller_id, seller_city, seller_state)
            VALUES (%s, %s, %s)
            ON CONFLICT (seller_id) DO UPDATE SET
                seller_city = EXCLUDED.seller_city,
                seller_state = EXCLUDED.seller_state
            """,
            (row.seller_id, row.get("seller_city"), row.get("seller_state")),
        )
    conn.commit()
    conn.close()


def load_dim_product():
    df = pd.read_parquet("data/bronze/structured/products.parquet")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_product
                (product_id, product_category_name_english, product_weight_g,
                 product_length_cm, product_width_cm, product_height_cm)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                product_category_name_english = EXCLUDED.product_category_name_english
            """,
            (
                row.product_id,
                row.get("product_category_name_english"),
                row.get("product_weight_g"),
                row.get("product_length_cm"),
                row.get("product_width_cm"),
                row.get("product_height_cm"),
            ),
        )
    conn.commit()
    conn.close()


def load_dim_date():
    # derived from sales data rather than a separate source -- this is a
    # generated calendar dimension, not something with its own raw file
    sales = pd.read_parquet("data/silver/sales_clean.parquet")
    dates = pd.to_datetime(sales["date_key"].unique())
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for d in dates:
        cur.execute(
            """
            INSERT INTO dim_date (date_key, year, month, day, weekday)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date_key) DO NOTHING
            """,
            (d.date(), d.year, d.month, d.day, d.strftime("%A")),
        )
    conn.commit()
    conn.close()


def load_dim_order():
    # one row per unique order_id, derived from sales_clean -- this is what
    # lets fact_sales and fact_reviews relate many-to-one to a shared
    # dimension instead of many-to-many to each other
    df = pd.read_parquet("data/silver/sales_clean.parquet")
    orders = df[["order_id", "customer_id", "date_key"]].drop_duplicates(subset=["order_id"])

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _, row in orders.iterrows():
        cur.execute(
            """
            INSERT INTO dim_order (order_id, customer_id, date_key)
            VALUES (%s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                date_key = EXCLUDED.date_key
            """,
            (row.order_id, row.customer_id, row.date_key),
        )
    conn.commit()
    conn.close()


def load_fact_sales():
    df = pd.read_parquet("data/silver/sales_clean.parquet")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO fact_sales
                (order_id, order_item_id, customer_id, product_id, seller_id,
                 date_key, price, freight_value, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, order_item_id) DO UPDATE SET
                price = EXCLUDED.price,
                freight_value = EXCLUDED.freight_value,
                updated_at = EXCLUDED.updated_at
            """,
            (
                row.order_id, int(row.order_item_id), row.customer_id,
                row.product_id, row.seller_id, row.date_key,
                float(row.price), float(row.freight_value),
                row.order_purchase_timestamp,
            ),
        )
    conn.commit()
    conn.close()


def load_fact_reviews():
    df = pd.read_parquet("data/silver/reviews_clean.parquet")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO fact_reviews
                (review_id, order_id, review_score, review_comment_message,
                 review_word_count, review_creation_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO UPDATE SET
                review_score = EXCLUDED.review_score,
                review_comment_message = EXCLUDED.review_comment_message,
                review_word_count = EXCLUDED.review_word_count
            """,
            (
                row.review_id, row.order_id, int(row.review_score),
                row.review_comment_message, int(row.review_word_count),
                row.review_creation_date,
            ),
        )
    conn.commit()
    conn.close()


def export_gold_parquet():
    # Mirror the warehouse gold tables to data/gold/*.parquet for file-based
    # consumers. The DB stays the source of truth; this is a convenience copy
    # and runs last, after the data-quality gate.
    os.makedirs("data/gold", exist_ok=True)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for table in GOLD_TABLES:
        cur.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
        df.to_parquet(f"data/gold/{table}.parquet", index=False)
    conn.close()
    print(f"gold export: {len(GOLD_TABLES)} tables -> data/gold/")


if __name__ == "__main__":
    # order matters: dims and dim_date before facts, since facts have FKs.
    # load_dim_order needs dim_customer + dim_date loaded first (its own FKs).
    load_dim_customer()
    load_dim_seller()
    load_dim_product()
    load_dim_date()
    load_dim_order()
    load_fact_sales()
    load_fact_reviews()
    export_gold_parquet()