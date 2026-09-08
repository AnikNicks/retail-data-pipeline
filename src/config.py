from dotenv import load_dotenv
import os

load_dotenv()  # reads .env if present -- never commit real credentials


def _cfg(primary: str, legacy: str):
    # Prefer RETAIL_DB_* (used inside the Airflow containers, where bare
    # DB_HOST/DB_PORT collide with Airflow's own entrypoint checks); fall
    # back to DB_* so local venv runs with the plain .env keep working.
    return os.getenv(primary) or os.getenv(legacy)


DB_CONFIG = {
    "host": _cfg("RETAIL_DB_HOST", "DB_HOST"),
    "port": _cfg("RETAIL_DB_PORT", "DB_PORT"),
    "dbname": _cfg("RETAIL_DB_NAME", "DB_NAME"),
    "user": _cfg("RETAIL_DB_USER", "DB_USER"),
    "password": _cfg("RETAIL_DB_PASSWORD", "DB_PASSWORD"),
}
