import pandas as pd
from src.watermark.watermark_utils import get_last_watermark, update_watermark

RAW_PATH = "data/raw/olist/olist_order_items_dataset.csv"
BRONZE_PATH = "data/bronze/structured/order_items.parquet"


def ingest_order_items():
    # order_items has its own timestamp (shipping_limit_date) -- it's not
    # the same as the order's purchase timestamp, so it gets its own
    # watermark rather than reusing "orders".
    df = pd.read_csv(RAW_PATH, parse_dates=["shipping_limit_date"])

    last_wm = get_last_watermark("order_items")
    new_rows = df[df["shipping_limit_date"] > last_wm]

    if new_rows.empty:
        print("order_items: no new rows.")
        return

    new_rows.to_parquet(BRONZE_PATH, index=False)
    update_watermark("order_items", new_rows["shipping_limit_date"].max())
    print(f"order_items: ingested {len(new_rows)} rows.")


if __name__ == "__main__":
    ingest_order_items()