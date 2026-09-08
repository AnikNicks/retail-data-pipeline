import pandas as pd
from src.watermark.watermark_utils import get_last_watermark, update_watermark

RAW_PATH = "data/raw/olist/olist_order_reviews_dataset.csv"
BRONZE_PATH = "data/bronze/unstructured/reviews.parquet"


def ingest_reviews():
    df = pd.read_csv(RAW_PATH, parse_dates=["review_creation_date"])

    last_wm = get_last_watermark("reviews")
    new_rows = df[df["review_creation_date"] > last_wm]

    if new_rows.empty:
        print("reviews: no new rows.")
        return

    # flag missing comments instead of dropping them -- a review with no
    # text is still a real, useful data point (e.g. score-only feedback)
    new_rows["has_comment"] = new_rows["review_comment_message"].notna()

    new_rows.to_parquet(BRONZE_PATH, index=False)
    update_watermark("reviews", new_rows["review_creation_date"].max())
    print(f"reviews: ingested {len(new_rows)} rows.")


if __name__ == "__main__":
    ingest_reviews()