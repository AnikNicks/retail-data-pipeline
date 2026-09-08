from src.transforms.spark_utils import get_spark, write_overwrite_parquet

# pyspark.sql.functions is imported inside each function below so Airflow's
# DAG parser can import this module without pyspark installed.


def clean_review_text(df):
    # empty string instead of null -- makes downstream word-count logic
    # simpler (no null-checking needed at every step)
    from pyspark.sql.functions import col, when

    return df.withColumn(
        "review_comment_message",
        when(col("review_comment_message").isNull(), "").otherwise(col("review_comment_message")),
    )


def compute_word_count(df):
    from pyspark.sql.functions import col, size, split, when

    return df.withColumn(
        "review_word_count",
        when(col("review_comment_message") == "", 0).otherwise(size(split(col("review_comment_message"), " "))),
    )


def clean_reviews():
    spark = get_spark()
    df = spark.read.parquet("data/bronze/unstructured/reviews.parquet")

    df = df.dropDuplicates(["review_id"])
    df = clean_review_text(df)
    df = compute_word_count(df)

    write_overwrite_parquet(df, "data/silver/reviews_clean.parquet")
    spark.stop()


if __name__ == "__main__":
    clean_reviews()