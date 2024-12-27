from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lag,
    sum as _sum,
    avg,
    count,
    when,
    unix_timestamp,
    expr,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)
from datetime import date, timedelta

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CalculateSessionMetricsOptimized") \
        .config("spark.sql.shuffle.partitions", "25") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # Define file paths
    input_user_interactions_path = "/opt/spark/dataset/output/user_interactions_sample.parquet"

    # Load data
    df = spark.read.parquet(input_user_interactions_path)

    # Define time gap threshold (e.g., 30 minutes in seconds)
    session_gap = 30 * 60

    # Define a window for session calculations
    window_spec = Window.partitionBy("user_id").orderBy("timestamp")

    # Compute time difference between consecutive events and mark new sessions
    data_with_sessions = df \
        .withColumn("end_timestamp", col("timestamp").cast("long") + (col("duration_ms") / 1000)) \
        .withColumn("time_diff", col("timestamp").cast("long") - lag("end_timestamp").over(window_spec)) \
        .withColumn("is_new_session", when(col("time_diff").isNull() | (col("time_diff") > session_gap), 1).otherwise(0)) \
        .withColumn("session_id", _sum("is_new_session").over(window_spec)) \
        .withColumn("time_diff", when(col("is_new_session") == 1, 0).otherwise(col("time_diff")))

    # Calculate session-level metrics
    session_metrics = data_with_sessions \
        .groupBy("user_id", "session_id") \
        .agg(
            (_sum("duration_ms") / 1000 + _sum("time_diff")).alias("session_duration_sec"),  # Sum of action durations + time_diff
            count("action_type").alias("actions_per_session")
        )

    # Calculate user-level metrics: average session duration and average actions per session
    user_metrics = session_metrics \
        .groupBy("user_id") \
        .agg(
            avg("session_duration_sec").alias("avg_session_duration_sec"),
            avg("actions_per_session").alias("avg_actions_per_session"),
        )

    # Show results
    user_metrics.show()

    spark.stop()
