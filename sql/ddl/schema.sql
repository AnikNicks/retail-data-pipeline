-- Reference/lookup tables (dimensions). No timestamp columns exist in the
-- raw Olist files for these, so they're full-reloaded rather than
-- watermarked -- see ingest_dimensions.py.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    weekday VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR PRIMARY KEY,
    customer_city VARCHAR,
    customer_state VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id VARCHAR PRIMARY KEY,
    seller_city VARCHAR,
    seller_state VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR PRIMARY KEY,
    product_category_name_english VARCHAR,  -- joined in from the translation file
    product_weight_g NUMERIC,
    product_length_cm NUMERIC,
    product_width_cm NUMERIC,
    product_height_cm NUMERIC
);

-- Bridge dimension: one row per unique order_id. fact_sales (line-item
-- grain) and fact_reviews (review grain) each relate many-to-one to this
-- instead of many-to-many directly to each other -- which would otherwise
-- let BI tools double-count or misalign review scores across line items.
CREATE TABLE IF NOT EXISTS dim_order (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR REFERENCES dim_customer(customer_id),
    date_key DATE REFERENCES dim_date(date_key)
);

-- Grain: one row per order LINE ITEM, not per order -- an order can have
-- multiple products, and price/freight/product/seller all live at the
-- item level in the source data.
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id VARCHAR NOT NULL,
    order_item_id INT NOT NULL,
    customer_id VARCHAR REFERENCES dim_customer(customer_id),
    product_id VARCHAR REFERENCES dim_product(product_id),
    seller_id VARCHAR REFERENCES dim_seller(seller_id),
    date_key DATE REFERENCES dim_date(date_key),
    price NUMERIC,
    freight_value NUMERIC,
    updated_at TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

-- Separate fact table from fact_sales: not every order has a review, and
-- a review has its own timestamp independent of the purchase date.
CREATE TABLE IF NOT EXISTS fact_reviews (
    review_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    review_score INT,
    review_comment_message TEXT,
    review_word_count INT,
    review_creation_date TIMESTAMP
);

-- Tracks the last successfully-processed timestamp per source, so a
-- pipeline restart resumes instead of reprocessing everything.
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    source_name VARCHAR PRIMARY KEY,
    last_watermark TIMESTAMP,
    last_run_at TIMESTAMP
);