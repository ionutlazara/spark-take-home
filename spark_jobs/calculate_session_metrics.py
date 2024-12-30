from config import dataset_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, expr, lag
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import unix_timestamp, when
from pyspark.sql.window import Window

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("CalculateSessionMetrics").getOrCreate()

    # Define file paths
    input_user_interactions_path = dataset_schema.input_user_interactions_path
    schema_user_interactions = dataset_schema.schema_user_interactions

    # Read csv into dataframe
    df = spark.read.csv(
        input_user_interactions_path, schema=schema_user_interactions, header=True
    )

    # Define time gap threshold (e.g., 30 minutes in seconds)
    session_gap = 30 * 60

    # Calculate "end timestamp" for each action
    df = df.withColumn(
        "end_timestamp", expr("unix_timestamp(timestamp) + duration_ms / 1000")
    )

    # Define a window for session calculations
    window_spec = Window.partitionBy("user_id").orderBy("timestamp")

    # Calculate the time difference considering the end timestamp of the previous action
    df_with_diff = df.withColumn(
        "time_diff",
        unix_timestamp(col("timestamp")) - lag("end_timestamp").over(window_spec),
    )

    # Identify new sessions by marking rows where the time difference exceeds the threshold
    df_with_sessions = df_with_diff.withColumn(
        "is_new_session",
        when(col("time_diff").isNull() | (col("time_diff") > session_gap), 1).otherwise(
            0
        ),
    )

    # Assign session IDs by cumulatively summing the is_new_session flag
    df_with_sessions = df_with_sessions.withColumn(
        "session_id", _sum("is_new_session").over(window_spec)
    )

    # Replace null time_diff values with 0 for session-level calculations
    df_with_sessions = df_with_sessions.withColumn(
        "time_diff",
        when(col("time_diff").isNull() | (col("time_diff") > session_gap), 0).otherwise(
            col("time_diff")
        ),
    )

    # Calculate session-level metrics
    session_metrics = df_with_sessions.groupBy("user_id", "session_id").agg(
        (_sum("duration_ms") / 1000 + _sum("time_diff")).alias(
            "session_duration_sec"
        ),  # Sum of action durations + time_diff
        count("action_type").alias("actions_per_session"),
    )

    # Calculate user-level metrics: average session duration and average actions per session
    user_metrics = session_metrics.groupBy("user_id").agg(
        avg("session_duration_sec").alias("avg_session_duration_sec"),
        avg("actions_per_session").alias("avg_actions_per_session"),
    )

    # Show results
    user_metrics.show()

    spark.stop()
