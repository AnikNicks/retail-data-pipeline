import psycopg2
from datetime import datetime
from src.config import DB_CONFIG


def get_last_watermark(source_name: str) -> datetime:
    """Returns the last successfully-processed timestamp for a source,
    or an old default if this source has never run before."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT last_watermark FROM pipeline_watermarks WHERE source_name = %s",
        (source_name,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else datetime(1970, 1, 1)  # no prior run -> pull everything


def update_watermark(source_name: str, new_watermark) -> None:
    """Upsert so this works on both the first run and every run after."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_watermarks (source_name, last_watermark, last_run_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (source_name)
        DO UPDATE SET last_watermark = EXCLUDED.last_watermark, last_run_at = NOW()
        """,
        (source_name, new_watermark),
    )
    conn.commit()
    conn.close()