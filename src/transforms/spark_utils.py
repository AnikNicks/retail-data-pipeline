import os
import shutil


def get_spark():
    # local[*] uses all available cores
    # imported inside the function so Airflow's DAG parser doesn't need
    # pyspark installed just to import the module
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("retail-pipeline")
        .master("local[*]")
        .getOrCreate()
    )


def write_overwrite_parquet(df, path):
    """Replace `path` with `df` written as Parquet.

    Spark's own mode("overwrite") can leave stale part-files from a prior
    run when the output directory sits on a bind mount (observed on WSL2):
    the two write jobs use different file UUIDs, the old files survive, and
    any later read of `path` returns both runs' rows. Deleting the target
    first makes the write deterministic; mode("overwrite") stays as a
    backstop.
    """
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.exists(path):
        os.remove(path)
    df.write.mode("overwrite").parquet(path)
