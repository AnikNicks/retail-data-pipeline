import psycopg2
from src.config import DB_CONFIG

# Runtime checks -- run every time the pipeline executes, not just in CI.
# These decide whether gold data is trustworthy enough to hand to BI.


def run_checks():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM fact_sales WHERE product_id IS NULL")
    assert cur.fetchone()[0] == 0, "FAIL: fact_sales rows missing product_id"

        # A review pointing at an order that isn't in fact_sales usually means
    # a pipeline bug -- BUT investigation (see README) confirmed this
    # dataset has a real, expected pattern: orders with status
    # 'unavailable'/'canceled' have zero order_items (correctly excluded by
    # the inner join in clean_sales.py) yet can still carry a review.
    # Baseline from investigation: 725 orphans, ~99% unavailable/canceled.
    cur.execute(
        """
        SELECT COUNT(*) FROM fact_reviews r
        LEFT JOIN fact_sales s ON r.order_id = s.order_id
        WHERE s.order_id IS NULL
        """
    )
    orphans = cur.fetchone()[0]
    assert orphans <= 800, (
        f"FAIL: {orphans} orphaned reviews -- exceeds expected baseline (~725, "
        f"documented in README). Investigate before raising this threshold."
    )

    print("All gold-layer quality checks passed.")
    conn.close()


if __name__ == "__main__":
    run_checks()