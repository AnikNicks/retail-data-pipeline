import pandas as pd

RAW_DIR = "data/raw/olist"
BRONZE_DIR = "data/bronze/structured"


def ingest_dimensions():
    # customers, sellers, and products have no timestamp column in the
    # raw files, so incremental/watermark logic doesn't apply here --
    # full reload is the correct approach for reference data this size.
    customers = pd.read_csv(f"{RAW_DIR}/olist_customers_dataset.csv")
    customers.to_parquet(f"{BRONZE_DIR}/customers.parquet", index=False)

    sellers = pd.read_csv(f"{RAW_DIR}/olist_sellers_dataset.csv")
    sellers.to_parquet(f"{BRONZE_DIR}/sellers.parquet", index=False)

    # categories ship in Portuguese -- join the translation file now so
    # downstream consumers (dashboards) never have to deal with it
    products = pd.read_csv(f"{RAW_DIR}/olist_products_dataset.csv")
    translation = pd.read_csv(f"{RAW_DIR}/product_category_name_translation.csv")
    products = products.merge(translation, on="product_category_name", how="left")
    products.to_parquet(f"{BRONZE_DIR}/products.parquet", index=False)

    print(f"dimensions: {len(customers)} customers, {len(sellers)} sellers, {len(products)} products.")


if __name__ == "__main__":
    ingest_dimensions()