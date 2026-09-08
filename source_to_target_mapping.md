# Source-to-Target Mapping

Column-level lineage for the `retail_pipeline` DAG:
raw Olist CSV → **Bronze** (Parquet) → **Silver** (Parquet) → **Gold** (`retail_dw` star schema).

| Layer | Location | Tool | Load pattern |
|---|---|---|---|
| Raw | `data/raw/olist/*.csv` | — | Kaggle download |
| Bronze | `data/bronze/{structured,unstructured}/*.parquet` | pandas + PyArrow | incremental (watermark) for facts, full reload for dims |
| Silver | `data/silver/{sales_clean,reviews_clean}.parquet` | PySpark | full rebuild each run |
| Gold | `retail_dw` tables (+ `data/gold/*.parquet` mirror) | pandas + psycopg2 | idempotent upsert (`INSERT … ON CONFLICT DO UPDATE`) |

## Incremental watermarks

Each fact source is filtered on `<column> > last_watermark(source)`; the high-water mark is
stored in `retail_dw.pipeline_watermarks`. Dimension sources have no timestamp and are fully
reloaded every run.

| Source CSV | Watermark column |
|---|---|
| `olist_orders_dataset.csv` | `order_purchase_timestamp` |
| `olist_order_items_dataset.csv` | `shipping_limit_date` |
| `olist_order_reviews_dataset.csv` | `review_creation_date` |

---

## `dim_customer`
`olist_customers_dataset.csv` → `bronze/structured/customers.parquet` → `dim_customer`

| Target column | Source column | Transformation |
|---|---|---|
| `customer_id` | `customer_id` | passthrough (PK) |
| `customer_city` | `customer_city` | passthrough |
| `customer_state` | `customer_state` | passthrough |

Not loaded: `customer_unique_id`, `customer_zip_code_prefix`.

## `dim_seller`
`olist_sellers_dataset.csv` → `bronze/structured/sellers.parquet` → `dim_seller`

| Target column | Source column | Transformation |
|---|---|---|
| `seller_id` | `seller_id` | passthrough (PK) |
| `seller_city` | `seller_city` | passthrough |
| `seller_state` | `seller_state` | passthrough |

Not loaded: `seller_zip_code_prefix`.

## `dim_product`
`olist_products_dataset.csv` LEFT JOIN `product_category_name_translation.csv`
on `product_category_name` → `bronze/structured/products.parquet` → `dim_product`

| Target column | Source column | Transformation |
|---|---|---|
| `product_id` | `product_id` | passthrough (PK) |
| `product_category_name_english` | `product_category_name_english` | joined in from the translation file (Portuguese → English) |
| `product_weight_g` | `product_weight_g` | passthrough |
| `product_length_cm` | `product_length_cm` | passthrough |
| `product_width_cm` | `product_width_cm` | passthrough |
| `product_height_cm` | `product_height_cm` | passthrough |

Not loaded: `product_category_name` (Portuguese), `product_name_lenght`,
`product_description_lenght`, `product_photos_qty`.

## `dim_date`
Generated from the distinct `date_key` values in `silver/sales_clean.parquet` — no raw source.

| Target column | Derivation |
|---|---|
| `date_key` | `to_date(orders.order_purchase_timestamp)`, distinct values (PK) |
| `year` / `month` / `day` | date parts of `date_key` |
| `weekday` | `date_key.strftime("%A")` |

Upsert: `ON CONFLICT (date_key) DO NOTHING`.

## `dim_order` *(bridge)*
`silver/sales_clean.parquet`, deduplicated to one row per `order_id`.

| Target column | Source column | Transformation |
|---|---|---|
| `order_id` | `order_id` | `drop_duplicates(subset=["order_id"])` (PK) |
| `customer_id` | `customer_id` | passthrough — FK → `dim_customer` |
| `date_key` | `date_key` | passthrough — FK → `dim_date` |

Purpose: lets `fact_sales` (line-item grain) and `fact_reviews` (review grain) each relate
**many-to-one** to a shared order key instead of many-to-many to each other.

## `fact_sales`
**Grain: one row per order line item** (`order_id` + `order_item_id`).
`olist_order_items_dataset.csv` + `olist_orders_dataset.csv` → bronze →
`silver/sales_clean.parquet` (`clean_sales.py`) → `fact_sales`.

Silver build: dedup `order_items` on `(order_id, order_item_id)`; dedup `orders` on `order_id`;
null `price`/`freight_value` → `0`; **inner join** `order_items ⋈ orders` on `order_id`
(orphan items dropped); `date_key = to_date(order_purchase_timestamp)`.

| Target column | Source column | Transformation |
|---|---|---|
| `order_id` | `order_items.order_id` | PK part |
| `order_item_id` | `order_items.order_item_id` | `int()`, PK part |
| `customer_id` | `orders.customer_id` | via join on `order_id` — FK → `dim_customer` |
| `product_id` | `order_items.product_id` | passthrough — FK → `dim_product` |
| `seller_id` | `order_items.seller_id` | passthrough — FK → `dim_seller` |
| `date_key` | derived | `to_date(orders.order_purchase_timestamp)` — FK → `dim_date` |
| `price` | `order_items.price` | `na.fill(0)` → `float()` |
| `freight_value` | `order_items.freight_value` | `na.fill(0)` → `float()` |
| `updated_at` | `orders.order_purchase_timestamp` | passthrough |

Upsert: `ON CONFLICT (order_id, order_item_id) DO UPDATE` → `price`, `freight_value`, `updated_at`.
Not loaded: `order_items.shipping_limit_date`, `orders.order_status` and delivery timestamps.

## `fact_reviews`
**Grain: one row per review** (`review_id`).
`olist_order_reviews_dataset.csv` → `bronze/unstructured/reviews.parquet` →
`silver/reviews_clean.parquet` (`clean_reviews.py`) → `fact_reviews`.

Silver build: dedup on `review_id`; null `review_comment_message` → `""`;
`review_word_count` = word count of the comment (`0` when empty).

| Target column | Source column | Transformation |
|---|---|---|
| `review_id` | `review_id` | dedup key (PK) |
| `order_id` | `order_id` | passthrough — soft link to `dim_order`; ~725 reviews reference orders with no line items (canceled/unavailable) |
| `review_score` | `review_score` | `int()` |
| `review_comment_message` | `review_comment_message` | `null → ""` |
| `review_word_count` | derived | `size(split(review_comment_message, " "))`, `0` if empty |
| `review_creation_date` | `review_creation_date` | passthrough; also the ingestion watermark |

Upsert: `ON CONFLICT (review_id) DO UPDATE` → `review_score`, `review_comment_message`, `review_word_count`.
Not loaded: `review_comment_title`, `review_answer_timestamp`.

---

## Sources not used

| CSV | Reason |
|---|---|
| `olist_order_payments_dataset.csv` | out of scope — no payments fact |
| `olist_geolocation_dataset.csv` | out of scope — no geo enrichment |
