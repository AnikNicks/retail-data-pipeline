import pandas as pd
from src.watermark.watermark_utils import get_last_watermark, update_watermark

RAW_PATH = "data/raw/olist/olist_orders_dataset.csv"
BRONZE_PATH = "data/bronze/structured/orders.parquet"


def ingest_orders():
    df = pd.read_csv(RAW_PATH, parse_dates=["order_purchase_timestamp"])

    last_wm = get_last_watermark("orders")
    new_rows = df[df["order_purchase_timestamp"] > last_wm]

    if new_rows.empty:
        print("orders: no new rows.")
        return

    new_rows.to_parquet(BRONZE_PATH, index=False)
    update_watermark("orders", new_rows["order_purchase_timestamp"].max())
    print(f"orders: ingested {len(new_rows)} rows.")


if __name__ == "__main__":
    ingest_orders()